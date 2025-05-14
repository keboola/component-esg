"""
Template Component main class.

"""

import csv
import logging
from io import StringIO
import requests

from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement
from wurlitzer import pipes

# from components.common.src.esg_client import EsgClient
from common.src.esg_client import EsgClient
from configuration import Configuration


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self.client = None

    def run(self):
        self.client = EsgClient(self.environment_variables.component_id, self.refresh_tokens())

        endpoint_to_method = {
            "franchises": self.import_franchises_ui_data,
            "intensity_metrics": self.import_intensity_metrics_ui_data,
            "water_storage": self.import_water_storage_ui_data,
            "employee_benefits": self.import_employee_benefits_ui_data,
            "social_protection": self.import_social_protection_ui_data,
            "locations": self.import_locations_ui_data,
            "non_compliance": self.import_non_compliance_ui_data,
            "generic": self.import_generic_data,
        }

        in_tables = self.get_input_tables_definitions()

        if self.params.endpoint == "investments":
            if len(in_tables) != 2:
                raise UserException(
                    "Investments endpoints needs 2 tables in input mapping: project finance, equity investments "
                )

            investments_table = [
                table for table in in_tables if "share_of_equity" in table.schema
            ][0]
            investments_data = []
            with open(investments_table.full_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    investments_data.append(row)

            finance_table = [
                table
                for table in in_tables
                if "share_of_total_project_cost" in table.schema
            ][0]
            finance_data = []
            with open(finance_table.full_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    finance_data.append(row)

            self.import_investments_ui_data(
                entity_id=self.params.entity_id,
                reporting_period_id=self.params.reporting_period_id,
                investments_data=investments_data,
                finance_data=finance_data,
            )

        else:
            if len(in_tables) != 1:
                raise UserException("Please provide exactly 1 table in input mapping.")

            data = []
            with open(in_tables[0].full_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    data.append(row)

            endpoint_to_method[self.params.endpoint](
                entity_id=self.params.entity_id,
                reporting_period_id=self.params.reporting_period_id,
                data=data,
            )

    def refresh_tokens(self) -> str:
        statefile = self.get_state_file()
        if (
            statefile.get("#refresh_token")
            and statefile.get("auth_id") == self.configuration.oauth_credentials.id
        ):
            logging.debug("Using refresh token from state file")
            refresh_token = statefile.get("#refresh_token")
        else:
            logging.debug("Using refresh token from configuration")
            refresh_token = self.configuration.oauth_credentials.data.get(
                "refresh_token"
            )

        client_id = self.configuration.oauth_credentials.appKey
        client_secret = self.configuration.oauth_credentials.appSecret

        url = "https://login.microsoftonline.com/277a3012-4462-4bb3-90ee-986a2006ebeb/oauth2/v2.0/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        response = requests.post(url, headers=headers, data=payload)
        if response.status_code != 200:
            raise UserException(
                f"Unable to refresh access token. Status code: {response.status_code} "
                f"Reason: {response.reason}, message: {response.json()}"
            )
        data = response.json()
        self.write_state_file(
            {
                "#refresh_token": data["refresh_token"],
                "auth_id": self.configuration.oauth_credentials.id,
            }
        )
        return data["id_token"]

    def import_franchises_ui_data(self, entity_id, reporting_period_id, data):
        result = self.client.import_franchises_ui_data(
            entity_id=entity_id,
            reporting_period_id=reporting_period_id,
            franchises_data=data,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_intensity_metrics_ui_data(
        self, entity_id: int, reporting_period_id: int, data: list
    ):
        processed_data = []
        for row in data:
            processed_row = {}
            for key, value in row.items():
                if key in ("emission", "water", "energy"):
                    processed_row[key] = value.lower() == "true"
                elif key in ("totalValueReported", "reportedValueInHighClimateSectors"):
                    try:
                        processed_row[key] = float(value)
                    except ValueError:
                        processed_row[key] = 0.0
                else:
                    processed_row[key] = value
            processed_data.append(processed_row)

        logging.info(
            f"Importing {len(processed_data)} intensity metrics data to ESG API..."
        )
        result = self.client.import_intensity_metrics_ui_data(
            entity_id=entity_id,
            reporting_period_id=reporting_period_id,
            intensity_metrics_data=processed_data,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_investments_ui_data(
        self,
        entity_id: int,
        reporting_period_id: int,
        investments_data: list,
        finance_data: list,
    ):
        result = self.client.import_investments_ui_data(
            entity_id=entity_id,
            reporting_period_id=reporting_period_id,
            equity_investments_data=investments_data,
            project_finance_data=finance_data,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_water_storage_ui_data(
        self, entity_id: int, reporting_period_id: int, data: list
    ):
        result = self.client.import_water_storage_ui_data(
            entity_id=entity_id,
            reporting_period_id=reporting_period_id,
            water_storage_data=data,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_employee_benefits_ui_data(
        self, entity_id: int, reporting_period_id: int, data: list
    ):
        grouped_data = {}
        for row in data:
            location = row["location"]
            sig_location = row["significant_location"]

            if location not in grouped_data:
                grouped_data[location] = {}

            if sig_location not in grouped_data[location]:
                grouped_data[location][sig_location] = []

            grouped_data[location][sig_location].append(row)

        # Transform the grouped data into the API structure
        result_data = []
        benefit_types = [
            "disabilityCoverage",
            "healthCare",
            "lifeInsurance",
            "other",
            "parentalLeave",
            "retirementProvision",
            "stockOwnership",
        ]

        def create_empty_benefit_data():
            return {
                "fullTimeEmployeesWithPermanentContract": 0,
                "partTimeEmployeesWithPermanentContract": 0,
                "fullTimeEmployeesWithTemporaryContract": 0,
                "partTimeEmployeesWithTemporaryContract": 0,
            }

        for location, sig_locations in grouped_data.items():
            location_data = {"location": location, "significantLocations": []}

            for sig_location, rows in sig_locations.items():
                sig_location_data = {"significantLocation": sig_location}

                for benefit_type in benefit_types:
                    sig_location_data[benefit_type] = create_empty_benefit_data()

                for row in rows:
                    benefit_type = row["benefit_type"]
                    if benefit_type in benefit_types:
                        sig_location_data[benefit_type] = {
                            "fullTimeEmployeesWithPermanentContract": row["full_time_permanent"],
                            "partTimeEmployeesWithPermanentContract": row["part_time_permanent"],
                            "fullTimeEmployeesWithTemporaryContract": row["full_time_temporary"],
                            "partTimeEmployeesWithTemporaryContract": row["part_time_temporary"],
                        }  # fmt: skip

                location_data["significantLocations"].append(sig_location_data)

            result_data.append(location_data)

        logging.info(
            f"Importing employee benefits data for {len(result_data)} locations to ESG API..."
        )
        result = self.client.import_benefit_for_employees_ui_data(
            entity_id=entity_id,
            reporting_period_id=reporting_period_id,
            employee_benefits_data=result_data,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_social_protection_ui_data(
        self, entity_id: int, reporting_period_id: int, data: list
    ):
        location_groups = {}
        for row in data:
            location = row["location"]
            if location not in location_groups:
                location_groups[location] = []
            location_groups[location].append(row)

        result_data = []

        for location, rows in location_groups.items():
            recorded = rows[0]["recorded"].lower() == "true"

            location_data = {
                "location": location,
                "recorded": recorded,
                "countries": [],
            }

            country_groups = {}
            for row in rows:
                country_name = row["country_name"]
                if country_name not in country_groups:
                    country_groups[country_name] = []
                country_groups[country_name].append(row)

            for country_name, country_rows in country_groups.items():
                country_data = {"name": country_name, "type_of_contract": []}

                contract_groups = {}
                for row in country_rows:
                    contract_type = row["contract_type"]
                    if contract_type not in contract_groups:
                        contract_groups[contract_type] = row

                for contract_type, row in contract_groups.items():
                    contract_data = {
                        "name": contract_type,
                        "sickness": {
                            "employees": int(row["sickness_employees"]),
                            "other_worker": int(row["sickness_other_worker"]),
                        },
                        "employmentInjuryAndDisability": {
                            "employees": int(row["employment_injury_disability_employees"]),
                            "other_worker": int(row["employment_injury_disability_other_worker"]),
                        },
                        "parentalLeave": {
                            "employees": int(row["parental_leave_employees"]),
                            "other_worker": int(row["parental_leave_other_worker"]),
                        },
                        "unemploymentStartingFrom": {
                            "employees": int(row["unemployment_employees"]),
                            "other_worker": int(row["unemployment_other_worker"]),
                        },
                        "retirement": {
                            "employees": int(row["retirement_employees"]),
                            "other_worker": int(row["retirement_other_worker"]),
                        },
                    }  # fmt: skip

                    country_data["type_of_contract"].append(contract_data)

                location_data["countries"].append(country_data)

            result_data.append(location_data)

        logging.info(
            f"Importing social protection data for {len(result_data)} locations to ESG API..."
        )
        result = self.client.import_social_protection_ui_data(
            entity_id=entity_id,
            reporting_period_id=reporting_period_id,
            social_protection_data=result_data,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_non_compliance_ui_data(
        self, entity_id: int, reporting_period_id: int, data: list
    ):
        processed_data = []
        for row in data:
            processed_row = {}
            for key, value in row.items():
                if key == "NumberOfIncidents":
                    processed_row[key] = int(value)
                elif key == "MonetaryValue":
                    processed_row[key] = float(value)
                else:
                    processed_row[key] = value
            processed_data.append(processed_row)

        logging.info(
            f"Importing {len(processed_data)} non-compliance incidents to ESG API..."
        )
        result = self.client.import_non_compliance_ui_data(
            entity_id=entity_id,
            reporting_period_id=reporting_period_id,
            non_compliance_data=processed_data,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_locations_ui_data(
        self, entity_id: int, reporting_period_id: int, data: list
    ):
        processed_data = []
        for row in data:
            location_entry = {
                "location": row["location"],
                "enviromentalInputtemplateId": [int(x) for x in row["environmental_template_ids"].split(";")],
                "governanceInputtemplateId": [int(x) for x in row["governance_template_ids"].split(";")],
                "socialInputtemplateId": [int(x) for x in row["social_template_ids"].split(";")],
            }  # fmt: skip
            processed_data.append(location_entry)

        logging.info(f"Importing {len(processed_data)} locations to ESG API...")
        result = self.client.import_locations_ui_data(
            entity_id=entity_id,
            reporting_period_id=reporting_period_id,
            locations_data=processed_data,
            ignore_locations=False,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_generic_data(self, entity_id: int, reporting_period_id: int, data: list):
        result = self.client.import_generic_data(
            entity_id=entity_id,
            reporting_period_id=reporting_period_id,
            template_id=self.params.template_id,
            data=data,
        )
        logging.info(result)

    @sync_action("list_clients")
    def list_clients(self) -> list[SelectElement]:
        out = StringIO()
        with pipes(stdout=out, stderr=out):
            self.client = EsgClient(self.environment_variables.component_id, self.refresh_tokens())
            clients = self.client.get_clients()
            return [
                SelectElement(value=f"{client['id']}-{client['name']}")
                for client in clients
            ]

    @sync_action("list_entities_with_periods")
    def list_entities_with_periods(self) -> list[SelectElement]:
        out = StringIO()
        with pipes(stdout=out, stderr=out):
            self.client = EsgClient(self.environment_variables.component_id, self.refresh_tokens())
            data = self.client.get_entities_with_periods(self.params.client_id)

            return [
                SelectElement(value=f"{pid}-{pname}   {eid}-{ename}")
                for pid, pname in data["reportingPeriods"].items()
                for eid, ename in data["entities"].items()
            ]

    @sync_action("list_entities")
    def list_entities(self) -> list[SelectElement]:
        out = StringIO()
        with pipes(stdout=out, stderr=out):
            self.client = EsgClient(self.environment_variables.component_id, self.refresh_tokens())
            entities = self.client.get_entities(self.params.client_id)
            return [
                SelectElement(value=f"{value}-{label}")
                for value, label in entities.items()
            ]

    @sync_action("list_reporting_periods")
    def list_reporting_periods(self) -> list[SelectElement]:
        out = StringIO()
        with pipes(stdout=out, stderr=out):
            self.client = EsgClient(self.environment_variables.component_id, self.refresh_tokens())
            entities = self.client.get_reporting_periods(self.params.client_id)
            return [
                SelectElement(value=f"{value}-{label}")
                for value, label in entities.items()
            ]

    @sync_action("list_templates")
    def list_templates(self) -> list[SelectElement]:
        out = StringIO()
        with pipes(stdout=out, stderr=out):
            self.client = EsgClient(self.environment_variables.component_id, self.refresh_tokens())
            templates = self.client.get_template_structure()
            return [
                SelectElement(value=f"{val['templateId']}-{val['templateName']}")
                for val in templates
            ]


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
