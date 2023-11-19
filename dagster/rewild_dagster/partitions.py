from dagster import StaticPartitionsDefinition

state_partitions = StaticPartitionsDefinition(["ACT", "NSW", "NT", "QLD", "SA", "TAS", "VIC", "WA"])

