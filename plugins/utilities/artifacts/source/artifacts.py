import abc
from typing import AnyStr, List, Dict


class SparkArtifactFactory:

    @staticmethod
    def create_jar_artifact(artifact_path: AnyStr, main_class_path: AnyStr,
                            main_parameters: Dict[AnyStr, List[AnyStr]]):
        return JarArtifact("JAR", artifact_path, main_class_path, main_parameters)

    @staticmethod
    def create_whl_artifact():
        raise NotImplementedError("Method not implemented")

    @staticmethod
    def create_zip_artifact():
        raise NotImplementedError("Method not implemented")

    @staticmethod
    def create_notebook_artifact():
        raise NotImplementedError("Method not implemented")


class SparkArtifact(metaclass=abc.ABCMeta):
    ARTIFACT_TYPES = ["JAR", "WHL", "EGG", "ZIP"]

    def __init__(self, artifact_type: AnyStr, artifactPath: AnyStr):
        if artifact_type not in self.ARTIFACT_TYPES:
            raise ValueError("Parameter Artifact Type {} is invalid, valid artifact types are {}".format(artifact_type,
                                                                                                         self.ARTIFACT_TYPES))

        self._artifact_type = artifact_type
        self._artifact_path = artifactPath
        self._libraries = []

    @property
    def artifact_type(self):
        return self._artifact_type

    @property
    def artifact_path(self):
        return self._artifact_path

    @property
    def libraries(self):
        return self._libraries


class JarArtifact(SparkArtifact):

    def __init__(self, _artifact_type: AnyStr, artifact_path: AnyStr, main_class_path: AnyStr,
                 parameters: Dict[AnyStr, List[AnyStr]]):
        super().__init__(_artifact_type, artifact_path)
        self._main_class_path = main_class_path
        self.libraries.append({_artifact_type.lower(): artifact_path})
        self._parameters = parameters

    @property
    def main_class_path(self):
        return self._main_class_path

    @property
    def parameters(self):
        return self._parameters
