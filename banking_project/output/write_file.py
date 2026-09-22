def write_parquet(df, output_path, mode = "overwrite"):
    try:
        df.write.mode(mode).parquet(output_path)
    except Exception as e:
        raise Exception(f"Error writing to {output_path}")

def write_delta(df, output_path, mode = "overwrite"):
    try:
        df.write.format("delta").mode(mode).save(output_path)
    except Exception as e:
        raise Exception(f"Error writing to {output_path}")        
    