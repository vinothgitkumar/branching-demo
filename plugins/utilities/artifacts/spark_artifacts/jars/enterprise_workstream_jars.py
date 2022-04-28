from plugins.utilities.artifacts.source.artifacts import SparkArtifactFactory

SPARROW_KAFKA_CONSUMER_JAR = SparkArtifactFactory. \
    create_jar_artifact(
        artifact_path="dbfs:/FileStore/artifact_repository/sparrow/jars/sparrow-consumer-bronze-assembly-0.1.jar",
        main_class_path="Driver", main_parameters={"staging": ["staging"], "production": ["production"]})
