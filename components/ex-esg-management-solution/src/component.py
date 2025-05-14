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

        templates = self.client.get_template_structure()

        if "lookup_tables" in self.params.endpoints:
            self.export_lookup_tables(templates)

        if "templates_structure" in self.params.endpoints:
            self.export_templates_structure(templates)

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

    def export_lookup_tables(self, templates) -> None:
        lookups = self.get_lookup_tables_names(templates)

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
            out_table = self.create_out_table_definition(
                name=f"lookup_table-{lookup.replace(' ', '_')}"
            )
            with open(out_table.full_path, "w", newline="") as out:
                writer = csv.writer(out)
                writer.writerow(["value"])
                for row in data:
                    writer.writerow([row])
            self.write_manifest(out_table)

    def get_lookup_tables_names(self, templates) -> set[str]:
        lookups = []

        for template in templates:
            for column in template["columnsConfiguration"]:
                if column["columnType"] == "Lookup":
                    if column.get("lookupName"):
                        lookups.append(column["lookupName"])
                    else:
                        logging.info(
                            f"Can't find lookup name for: {template.get('templateName')} for column: {column}"
                        )

        return set(lookups)

    def export_templates_structure(self, templates) -> None:
        """Export templates structure to CSV files.

        Creates a CSV file for each template with the format: template_templateid_template_name.csv
        The CSV contains the template's columns configuration with properties like columnType,
        dbColumnName, disableValidation, excelColumnName, isRequired, mustBeInPeriod,
        lookupName (for Lookup columns), and numberCondition (for numeric columns).

        Args:
            templates: List of template structures with their column configurations
        """

        for template in templates:
            template_id = template.get("templateId", "")
            template_name = template.get("templateName", "").replace(" ", "").replace(",", "")

            file_name = f"template_{template_id}-{template_name}.csv"
            out_table = self.create_out_table_definition(
                name=file_name,
                schema=[
                    "columnType",
                    "dbColumnName",
                    "disableValidation",
                    "excelColumnName",
                    "isRequired",
                    "mustBeInPeriod",
                    "lookupName",
                    "numberCondition",
                ],
            )

            with open(out_table.full_path, "w", newline="") as out:
                writer = csv.writer(out)
                writer.writerow(
                    [
                        "columnType",
                        "dbColumnName",
                        "disableValidation",
                        "excelColumnName",
                        "isRequired",
                        "mustBeInPeriod",
                        "lookupName",
                        "numberCondition",
                    ]
                )

                for column in template.get("columnsConfiguration", []):
                    row = [
                        column.get("columnType", ""),
                        column.get("dbColumnName", ""),
                        column.get("disableValidation", ""),
                        column.get("excelColumnName", ""),
                        column.get("isRequired", ""),
                        column.get("mustBeInPeriod", ""),
                        column.get("lookupName", ""),
                        column.get("numberCondition", ""),
                    ]
                    writer.writerow(row)

            self.write_manifest(out_table)

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
