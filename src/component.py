"""
Template Component main class.

"""

import csv
import logging
import requests

from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from esg.client import EsgClient

from configuration import Configuration


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self.client = EsgClient(self.refresh_tokens())

    def run(self):
        self.download_lookup_tables()

        # Define endpoint to method mapping
        endpoint_to_method = {
            "franchises": {
                "method": self.import_franchises_ui_data,
                "files": ["dummy_franchises_data.csv"],
                "args": [self.params.entity_id, self.params.client_reporting_period_id],
            },
            "intensity_metrics": {
                "method": self.import_intensity_metrics_ui_data,
                "files": ["intensity_metrics_sample.csv"],
                "args": [self.params.entity_id, self.params.client_reporting_period_id],
            },
            "investments": {
                "method": self.import_investments_ui_data,
                "files": ["equity_investments_sample.csv", "project_finance_sample.csv"],
                "args": [self.params.entity_id, self.params.client_reporting_period_id],
            },
            "water_storage": {
                "method": self.import_water_storage_ui_data,
                "files": ["water_storage_sample.csv"],
                "args": [self.params.entity_id, self.params.client_reporting_period_id],
            },
            "employee_benefits": {
                "method": self.import_employee_benefits_ui_data,
                "files": ["employee_benefits_sample.csv"],
                "args": [self.params.entity_id, self.params.client_reporting_period_id],
            },
            "social_protection": {
                "method": self.import_social_protection_ui_data,
                "files": ["social_protection_sample.csv"],
                "args": [self.params.entity_id, self.params.client_reporting_period_id],
            },
            "locations": {
                "method": self.import_locations_ui_data,
                "files": ["locations_sample.csv"],
                "args": [self.params.entity_id, self.params.client_reporting_period_id],
            },
            "non_compliance": {
                "method": self.import_non_compliance_ui_data,
                "files": ["non_compliance_sample.csv"],
                "args": [self.params.entity_id, self.params.client_reporting_period_id],
            },
            "generic": {
                "method": self.import_generic_data,
                "files": ["generic_16.csv"],
                "args": [self.params.entity_id, self.params.client_reporting_period_id, self.params.data_template],
            },
        }

        # Get the mapping for the specified endpoint
        if self.params.endpoint not in endpoint_to_method:
            raise UserException(
                f"Invalid endpoint: {self.params.endpoint}. Must be one of: {', '.join(endpoint_to_method.keys())}"
            )

        mapping = endpoint_to_method[self.params.endpoint]

        # Load data from CSV files
        loaded_data = []
        for file_name in mapping["files"]:
            file_data = []
            with open(f"../example-data/{file_name}", "r", encoding="utf-8") as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    file_data.append(row)
            loaded_data.append(file_data)

        # Call the import method with the loaded data
        if len(loaded_data) == 1:
            mapping["method"](*mapping["args"], loaded_data[0])
        else:
            mapping["method"](*mapping["args"], *loaded_data)

    def refresh_tokens(self):
        statefile = self.get_state_file()
        if statefile.get("#refresh_token") and statefile.get("auth_id") == self.configuration.oauth_credentials.id:
            logging.debug("Using refresh token from state file")
            refresh_token = statefile.get("#refresh_token")
        else:
            logging.debug("Using refresh token from configuration")
            refresh_token = self.configuration.oauth_credentials.data.get("refresh_token")

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
            {"#refresh_token": data["refresh_token"], "auth_id": self.configuration.oauth_credentials.id}
        )
        return data["id_token"]

    def download_lookup_tables(self):
        lookups = self.get_lookup_tables_names()

        lookups.update(
            [
                "NonCompliance-CategoryOfSanction",
                "ProjectFinanceAndDebtInvestment_InvestmentType",
                "EquityInvestment_InvestmentType",
                "TypeOfIntensityMetric",
            ]
        )

        for lookup in lookups:
            data = self.client.get_lookup_data(lookup)
            out_table = self.create_out_table_definition(name=f"lookup_table_{lookup}")
            with open(out_table.full_path, "w", newline="") as out:
                writer = csv.writer(out)
                writer.writerow(["value"])
                for row in data:
                    writer.writerow([row])
            self.write_manifest(out_table)

    def get_lookup_tables_names(self):
        lookups = []
        templates = self.client.get_template_structure()

        for template in templates:
            for column in template["columnsConfiguration"]:
                if column["columnType"] == "Lookup":
                    if column.get("lookupName"):
                        lookups.append(column["lookupName"])
                    else:
                        logging.info(f"Can't find lookup name for: {template.get('templateName')} for column: {column}")

        return set(lookups)

    def import_franchises_ui_data(self, entity_id, client_reporting_period_id, franchises_data):
        logging.info(f"Importing {len(franchises_data)} franchise data to ESG API...")
        result = self.client.import_franchises_ui_data(
            entity_id=entity_id,
            client_reporting_period_id=client_reporting_period_id,
            franchises_data=franchises_data,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_intensity_metrics_ui_data(
        self, entity_id: int, client_reporting_period_id: int, intensity_metrics_data: list
    ):
        """
        Import intensity metrics data to the ESG API.

        Args:
            entity_id: The entity ID
            client_reporting_period_id: The reporting period ID
            intensity_metrics_data: List of intensity metrics data to import
        """
        # Process the data
        processed_data = []
        for row in intensity_metrics_data:
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

        logging.info(f"Importing {len(processed_data)} intensity metrics data to ESG API...")
        result = self.client.import_intensity_metrics_ui_data(
            entity_id=entity_id,
            client_reporting_period_id=client_reporting_period_id,
            intensity_metrics_data=processed_data,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_investments_ui_data(
        self, entity_id: int, client_reporting_period_id: int, equity_investments_data: list, project_finance_data: list
    ):
        result = self.client.import_investments_ui_data(
            entity_id=entity_id,
            client_reporting_period_id=client_reporting_period_id,
            equity_investments_data=equity_investments_data,
            project_finance_data=project_finance_data,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_water_storage_ui_data(self, entity_id: int, client_reporting_period_id: int, water_storage_data: list):

        logging.info(f"Importing {len(water_storage_data)} water storage records to ESG API...")
        result = self.client.import_water_storage_ui_data(
            entity_id=entity_id,
            client_reporting_period_id=client_reporting_period_id,
            water_storage_data=water_storage_data,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_employee_benefits_ui_data(
        self, entity_id: int, client_reporting_period_id: int, employee_benefits_data: list
    ):
        grouped_data = {}
        for row in employee_benefits_data:
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
                        }

                location_data["significantLocations"].append(sig_location_data)

            result_data.append(location_data)

        logging.info(f"Importing employee benefits data for {len(result_data)} locations to ESG API...")
        result = self.client.import_benefit_for_employees_ui_data(
            entity_id=entity_id,
            client_reporting_period_id=client_reporting_period_id,
            employee_benefits_data=result_data,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_social_protection_ui_data(
        self, entity_id: int, client_reporting_period_id: int, social_protection_data: list
    ):

        location_groups = {}
        for row in social_protection_data:
            location = row["location"]
            if location not in location_groups:
                location_groups[location] = []
            location_groups[location].append(row)

        result_data = []

        for location, rows in location_groups.items():
            recorded = rows[0]["recorded"].lower() == "true"

            location_data = {"location": location, "recorded": recorded, "countries": []}

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
                    }

                    country_data["type_of_contract"].append(contract_data)

                location_data["countries"].append(country_data)

            result_data.append(location_data)

        logging.info(f"Importing social protection data for {len(result_data)} locations to ESG API...")
        result = self.client.import_social_protection_ui_data(
            entity_id=entity_id,
            client_reporting_period_id=client_reporting_period_id,
            social_protection_data=result_data,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_non_compliance_ui_data(self, entity_id: int, client_reporting_period_id: int, non_compliance_data: list):

        processed_data = []
        for row in non_compliance_data:
            processed_row = {}
            for key, value in row.items():
                if key == "NumberOfIncidents":
                    processed_row[key] = int(value)
                elif key == "MonetaryValue":
                    processed_row[key] = float(value)
                else:
                    processed_row[key] = value
            processed_data.append(processed_row)

        logging.info(f"Importing {len(processed_data)} non-compliance incidents to ESG API...")
        result = self.client.import_non_compliance_ui_data(
            entity_id=entity_id,
            client_reporting_period_id=client_reporting_period_id,
            non_compliance_data=processed_data,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_locations_ui_data(self, entity_id: int, client_reporting_period_id: int, locations_data: list):
        processed_data = []
        for row in locations_data:
            location_entry = {
                "location": row["location"],
                "enviromentalInputtemplateId": [int(x) for x in row["environmental_template_ids"].split(";")],
                "governanceInputtemplateId": [int(x) for x in row["governance_template_ids"].split(";")],
                "socialInputtemplateId": [int(x) for x in row["social_template_ids"].split(";")],
            }
            processed_data.append(location_entry)

        logging.info(f"Importing {len(processed_data)} locations to ESG API...")
        result = self.client.import_locations_ui_data(
            entity_id=entity_id,
            client_reporting_period_id=client_reporting_period_id,
            locations_data=processed_data,
            ignore_locations=False,
            data_not_available=False,
            data_not_available_comment=None,
        )
        logging.info(result)

    def import_generic_data(
        self, entity_id: int, client_reporting_period_id: int, template_id: int, generic_data: list
    ):
        logging.info(f"Importing {len(generic_data)} rows of generic data to ESG API...")
        result = self.client.import_generic_data(
            entity_id=entity_id,
            client_reporting_period_id=client_reporting_period_id,
            template_id=template_id,
            data=generic_data,
        )
        logging.info(result)

    @sync_action("list_entities")
    def list_entities(self):
        entities = self.client.get_entities(self.params.client_id)
        return [SelectElement(value, label) for value, label in entities.items()]

    @sync_action("list_reporting_periods")
    def list_reporting_periods(self):
        entities = self.client.get_reporting_periods(self.params.client_id)
        return [SelectElement(value, label) for value, label in entities.items()]

    @sync_action("list_templates")
    def list_templates(self):
        templates = self.client.get_template_structure()
        return [SelectElement(val["templateId"], val["templateName"]) for val in templates]


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
