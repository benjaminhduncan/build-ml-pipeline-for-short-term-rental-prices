#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases
"""
import argparse
import logging
import wandb
import pandas as pd
import tempfile

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)
    logger.info("Pulling artifact from W&B")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    # Form pandas dataframe from csv artifact
    df = pd.read_csv(artifact_local_path)
    # Drop outliers
    logger.info(f"Dropping outliers from artifact")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    logger.info(f"Converting datatype to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Save csv to temporary location
    with tempfile.NamedTemporaryFile(mode='wb+') as temp_csv:
        df.to_csv(temp_csv, index=False)
        # Make sure the file has been written to disk
        temp_csv.flush()
        logger.info("Creating artifact")
        artifact = wandb.Artifact(
            args.output_artifact,
            type=args.output_type,
            description=args.output_description,
        )
        artifact.add_file(temp_csv.name, name="clean_sample.csv")

        logger.info("Logging artifact")
        run.log_artifact(artifact)

        # Ensure artifact is uploaded before destroying temp_csv
        artifact.wait()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This step cleans the data")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Fully-qualitied name for the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name for the W&B artifact that will be created after cleaning",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the artifact to create",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the artifact to create",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="TThe minimum rental price to consider",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="The maximum rental price to consider",
        required=True
    )

    args = parser.parse_args()

    go(args)
