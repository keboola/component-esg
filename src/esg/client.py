from keboola.http_client import HttpClient
from keboola.component.exceptions import UserException
from typing import Dict, Any, List, Optional, Callable

BASE_URL = "https://esg-externalintegrationapi-keboola-stg.azurewebsites.net/api/"

ENDPOINT_GET_ENTITIES = "ExternalIntegration/ClientData/GetEntities"
ENDPOINT_GET_REPORTING_PERIODS = "ExternalIntegration/ClientData/GetReportingPeriods"
ENDPOINT_GET_LOOKUP_DATA = "ExternalIntegration/LookupData/GetLookupData"
ENDPOINT_GET_TEMPLATE_STRUCTURE = "ExternalIntegration/TemplateData/TemplatesStructure"

ENDPOINT_IMPORT_FRANCHISES_UI_DATA = "ExternalIntegration/TemplateData/ImportFranchisesUiData"
ENDPOINT_IMPORT_INTENSITY_METRICS_UI_DATA = "ExternalIntegration/TemplateData/ImportIntensityMetricsUiData"
ENDPOINT_IMPORT_INVESTMENTS_UI_DATA = "ExternalIntegration/TemplateData/ImportInvestmentsUiData"
ENDPOINT_IMPORT_WATER_STORAGE_UI_DATA = "ExternalIntegration/TemplateData/ImportWaterStorageUiData"
ENDPOINT_IMPORT_BENEFIT_FOR_EMPLOYEES_UI_DATA = "ExternalIntegration/TemplateData/ImportBenefitForEmployeesUiData"
ENDPOINT_IMPORT_SOCIAL_PROTECTION_UI_DATA = "ExternalIntegration/TemplateData/ImportSocialProtectionUiData"
ENDPOINT_IMPORT_NON_COMPLIANCE_UI_DATA = "ExternalIntegration/TemplateData/ImportNonComplianceUiData"
ENDPOINT_IMPORT_LOCATIONS_UI_DATA = "ExternalIntegration/TemplateData/ImportLocationsUiData"
ENDPOINT_IMPORT_GENERIC_DATA = "ExternalIntegration/TemplateData/ImportGenericData"

BATCHE_SIZE = 100


class EsgClient(HttpClient):
    """Client for interacting with the ESG External Integration API."""

    def __init__(self, id_token: Optional[str] = None):
        """Initialize the ESG client.

        Args:
            id_token: Bearer token for authentication
        """
        super().__init__(base_url=BASE_URL)
        if id_token:
            self.update_auth_header({"Authorization": f"Bearer {id_token}"})

    def _make_request(self, method: Callable, endpoint_path: str, error_message: str, **kwargs) -> Dict[str, Any]:
        """Make an HTTP request and handle common error cases.

        Args:
            method: The HTTP method to call (self.get_raw or self.post_raw)
            endpoint_path: The API endpoint path
            error_message: Custom error message for exceptions
            **kwargs: Additional arguments to pass to the HTTP method

        Returns:
            Dict containing the JSON response or a success message for empty responses

        Raises:
            UserException: If the request fails
        """
        try:
            response = method(endpoint_path=endpoint_path, **kwargs)
            response.raise_for_status()

            # Handle empty response
            if not response.text:
                return {"status": "success", "message": f"Request to {endpoint_path} completed successfully"}

            return response.json()
        except Exception as e:
            raise UserException(f"{error_message}: {e}")

    def _import_ui_data(
        self,
        endpoint: str,
        entity_id: int,
        client_reporting_period_id: int,
        template_data: Any,
        data_not_available: bool = False,
        data_not_available_comment: Optional[str] = None,
        **extra_fields,
    ) -> Dict[str, Any]:
        """Base method for importing UI data with common payload structure.

        Args:
            endpoint: The API endpoint to use
            entity_id: The entity ID
            client_reporting_period_id: The reporting period ID
            template_data: The data to import
            data_not_available: Whether data is not available
            data_not_available_comment: Comment for unavailable data
            **extra_fields: Additional fields to include in the payload

        Returns:
            Dict containing the response or success message
        """
        payload = {
            "entityId": entity_id,
            "clientReportingPeriodId": client_reporting_period_id,
            "templateData": template_data,
            "dataNotAvailable": data_not_available,
            "dataNotAvailableComment": data_not_available_comment,
            **extra_fields,
        }

        return self._make_request(self.post_raw, endpoint, f"Failed to import data to {endpoint}", json=payload)

    def get_entities(self, client_id: int) -> Dict[str, Any]:
        """Get entities for a client."""
        return self._make_request(
            self.get_raw, ENDPOINT_GET_ENTITIES, "Failed to retrieve entities", params={"clientId": client_id}
        )

    def get_reporting_periods(self, client_id: int) -> Dict[str, Any]:
        """Get reporting periods for a client."""
        return self._make_request(
            self.get_raw,
            ENDPOINT_GET_REPORTING_PERIODS,
            "Failed to retrieve reporting periods",
            params={"clientId": client_id},
        )

    def get_lookup_data(self, lookup_name: str) -> Dict[str, Any]:
        """Get lookup data by name."""
        return self._make_request(
            self.get_raw, ENDPOINT_GET_LOOKUP_DATA, "Failed to retrieve lookup data", params={"lookupName": lookup_name}
        )

    def get_template_structure(self) -> Dict[str, Any]:
        """Get template structure."""
        return self._make_request(
            self.get_raw, ENDPOINT_GET_TEMPLATE_STRUCTURE, "Failed to retrieve template structure"
        )

    def import_franchises_ui_data(
        self,
        entity_id: int,
        client_reporting_period_id: int,
        franchises_data: List[Dict[str, Any]],
        data_not_available: bool = False,
        data_not_available_comment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Import franchises UI data."""
        rows = [{"data": data, "index": i + 1} for i, data in enumerate(franchises_data)]
        template_data = {"franchisesTable": {"rows": rows}}

        return self._import_ui_data(
            ENDPOINT_IMPORT_FRANCHISES_UI_DATA,
            entity_id,
            client_reporting_period_id,
            template_data,
            data_not_available,
            data_not_available_comment,
        )

    def import_intensity_metrics_ui_data(
        self,
        entity_id: int,
        client_reporting_period_id: int,
        intensity_metrics_data: List[Dict[str, Any]],
        data_not_available: bool = False,
        data_not_available_comment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Import intensity metrics UI data."""
        return self._import_ui_data(
            ENDPOINT_IMPORT_INTENSITY_METRICS_UI_DATA,
            entity_id,
            client_reporting_period_id,
            intensity_metrics_data,
            data_not_available,
            data_not_available_comment,
        )

    def import_investments_ui_data(
        self,
        entity_id: int,
        client_reporting_period_id: int,
        equity_investments_data: List[Dict[str, Any]],
        project_finance_data: List[Dict[str, Any]],
        data_not_available: bool = False,
        data_not_available_comment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Import investments UI data."""
        template_data = {
            "equityInvestmentsTable": {
                "rows": [{"data": data, "index": i + 1} for i, data in enumerate(equity_investments_data)]
            },
            "projectFinanceTable": {
                "rows": [{"data": data, "index": i + 1} for i, data in enumerate(project_finance_data)]
            },
        }

        return self._import_ui_data(
            ENDPOINT_IMPORT_INVESTMENTS_UI_DATA,
            entity_id,
            client_reporting_period_id,
            template_data,
            data_not_available,
            data_not_available_comment,
        )

    def import_water_storage_ui_data(
        self,
        entity_id: int,
        client_reporting_period_id: int,
        water_storage_data: List[Dict[str, Any]],
        data_not_available: bool = False,
        data_not_available_comment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Import water storage UI data."""
        return self._import_ui_data(
            ENDPOINT_IMPORT_WATER_STORAGE_UI_DATA,
            entity_id,
            client_reporting_period_id,
            water_storage_data,
            data_not_available,
            data_not_available_comment,
        )

    def import_benefit_for_employees_ui_data(
        self,
        entity_id: int,
        client_reporting_period_id: int,
        employee_benefits_data: List[Dict[str, Any]],
        data_not_available: bool = False,
        data_not_available_comment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Import employee benefits UI data."""
        return self._import_ui_data(
            ENDPOINT_IMPORT_BENEFIT_FOR_EMPLOYEES_UI_DATA,
            entity_id,
            client_reporting_period_id,
            employee_benefits_data,
            data_not_available,
            data_not_available_comment,
        )

    def import_social_protection_ui_data(
        self,
        entity_id: int,
        client_reporting_period_id: int,
        social_protection_data: List[Dict[str, Any]],
        data_not_available: bool = False,
        data_not_available_comment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Import social protection UI data."""
        return self._import_ui_data(
            ENDPOINT_IMPORT_SOCIAL_PROTECTION_UI_DATA,
            entity_id,
            client_reporting_period_id,
            social_protection_data,
            data_not_available,
            data_not_available_comment,
        )

    def import_non_compliance_ui_data(
        self,
        entity_id: int,
        client_reporting_period_id: int,
        non_compliance_data: List[Dict[str, Any]],
        data_not_available: bool = False,
        data_not_available_comment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Import non-compliance UI data."""
        template_data = {"listOfIncidents": non_compliance_data}

        return self._import_ui_data(
            ENDPOINT_IMPORT_NON_COMPLIANCE_UI_DATA,
            entity_id,
            client_reporting_period_id,
            template_data,
            data_not_available,
            data_not_available_comment,
        )

    def import_locations_ui_data(
        self,
        entity_id: int,
        client_reporting_period_id: int,
        locations_data: List[Dict[str, Any]],
        ignore_locations: bool = False,
        data_not_available: bool = False,
        data_not_available_comment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Import locations UI data."""
        template_data = {"locations": locations_data}

        return self._import_ui_data(
            ENDPOINT_IMPORT_LOCATIONS_UI_DATA,
            entity_id,
            client_reporting_period_id,
            template_data,
            data_not_available,
            data_not_available_comment,
            ignoreLocations=ignore_locations,
        )

    def import_generic_data(
        self, entity_id: int, client_reporting_period_id: int, template_id: int, data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Import generic template data."""
        rows = [
            {"columns": [{"name": key, "value": str(value)} for key, value in row_data.items()]} for row_data in data
        ]

        template_data = {"rows": rows}

        return self._import_ui_data(
            ENDPOINT_IMPORT_GENERIC_DATA, entity_id, client_reporting_period_id, template_data, template_id=template_id
        )
