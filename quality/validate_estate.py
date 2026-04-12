import os
import logging
import json
import great_expectations as gx
from kestra import Kestra

logging.basicConfig(level=logging.INFO, format='%(message)s')

try:
    logging.info(json.dumps({"event": "gx_init", "message": "Starting Data Validation"}))

    context = gx.get_context(mode="ephemeral")

    datasource = context.sources.add_postgres(
        name="estate_postgres", 
        connection_string=os.environ["GX_DB_URL"]
    )

    asset = datasource.add_table_asset(
        name="clean_properties", 
        schema_name="curated_estate_schema"
    )

    batch_request = asset.build_batch_request()
    validator = context.get_validator(
        batch_request=batch_request, 
        create_expectation_suite_with_name="property_quality_suite"
    )

    validator.expect_column_values_to_not_be_null("price_numeric")
    validator.expect_column_values_to_be_between("price_numeric", min_value=1000)
    validator.expect_column_values_to_not_be_null("property_type")

    validation_result = validator.validate()

    if not validation_result.success:
        logging.error(json.dumps({
            "event": "validation_failed", 
            "details": validation_result.to_json_dict()
        }))
        Kestra.counter("gx_validation_failures", 1)
        raise Exception("Data Validation Failed! Halting pipeline to prevent bad data downstream.")

    logging.info(json.dumps({"event": "validation_success", "status": "passed"}))
    Kestra.counter("gx_validation_success", 1)

except Exception as e:
    logging.error(json.dumps({"event": "gx_fatal_error", "error": str(e)}))
    raise