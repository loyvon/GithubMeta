import os


class Configuration:
    DataDir = "data"

    GithubUsername = os.environ["GITHUB_USERNAME"]
    GithubToken = os.environ["GITHUB_TOKEN"]

    OpenaiApiType = os.environ["OPENAI_API_TYPE"]
    OpenaiApiKey = os.environ["OPENAI_API_KEY"]
    OpenaiApiVersion = os.environ["OPENAI_API_VERSION"]
    OpenaiAzureEndpoint = os.environ["OPENAI_AZURE_ENDPOINT"]
    OpenaiModel = os.environ["OPENAI_MODEL"]

    MysqlHost = os.environ["AZURE_MYSQL_HOST"]
    MysqlName = os.environ["AZURE_MYSQL_NAME"]
    MysqlPasswd = os.environ["AZURE_MYSQL_PASSWORD"]
    MysqlUser = os.environ["AZURE_MYSQL_USER"]
