import great_expectations as gx
import os
import logging
import json
import sys
from great_expectations.datasource.fluent.interfaces import TestConnectionError

logging.basicConfig(level=logging.INFO, format='%(message)s')

try:
    context = gx.get_context()
    gx_db_url = os.environ["GX_DB_URL"]

    datasource = context.sources.add_postgres(
        name="estate_postgres",
        connection_string=gx_db_url
    )

    try:
        asset = datasource.add_table_asset(
            name="clean_uk_properties",
            table_name="clean_uk_properties",
            schema_name="curated_estate_schema"
        )
    except TestConnectionError as e:
        if "does not exist" in str(e):
            logging.warning(json.dumps({
                "event": "day_0_handling",
                "message": "Curated table missing. Skipping validation."
            }))
            sys.exit(0)
        else:
            raise e

    batch_request = asset.build_batch_request()

    suite_name = "uk_estate_quality_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    #Expectations
    validator.expect_column_values_to_not_be_null("transaction_id")
    validator.expect_column_values_to_be_unique("transaction_id")
    validator.expect_column_values_to_not_be_null("price_numeric")
    validator.expect_column_values_to_be_between("price_numeric", min_value=1)

    validator.save_expectation_suite()

    checkpoint = context.add_or_update_checkpoint(
        name="estate_checkpoint",
        validations=[{
            "batch_request": batch_request,
            "expectation_suite_name": suite_name
        }]
    )

    result = checkpoint.run()

    if not result["success"]:
        logging.error(json.dumps({
            "event": "gx_failed",
            "details": str(result)
        }))
        sys.exit(1)

    logging.info(json.dumps({
        "event": "gx_success",
        "message": "All data quality checks passed"
    }))

except Exception as e:
    logging.error(json.dumps({
        "event": "gx_fatal_error",
        "error": str(e)
    }))
    raise