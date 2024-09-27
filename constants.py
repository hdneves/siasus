DIR_DOWNLOAD = '/databricks/driver/Downloads'

DEFAULT_DIR = '/root/documentos/SOW - PySUS'

DIR_LIB = '/home/hdrs/documentos/siasus/Downloads'

QUERY_SQL = """
    DECLARE @DataMax DATETIME;
    SELECT @DataMax = MAX(DATA_PROCESSAMENTO) FROM SUS_SIASUS;

    DECLARE @DataMin DATETIME;
    SET @DataMin = DATEADD(MONTH, -5, @DataMax);

    DELETE FROM SUS_SIASUS
    WHERE DATA_PROCESSAMENTO BETWEEN @DataMin AND @DataMax;
    """

STATES = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SE", "TO", "SP"
] #A List with all brazilian states.

CLASS_DICT = {
    'Processor': ['0701030330', '0701030348'],
    'Classe A': ['0701030038','0701030062','0701030097','0701030127','0701030186','0701030216','0701030240','0701030275'],
    'Classe B': ['0701030046','0701030070','0701030100','0701030135','0701030194','0701030224','0701030259','0701030283'],
    'Classe C': ['0701030054','0701030089','0701030119','0701030143','0701030208','0701030232','0701030267','0701030291'],
    'FM': ['0701030321']
}