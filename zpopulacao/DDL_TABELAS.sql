CREATE TABLE PUBLIC.CURATED_ESTABELECIMENTOS(
CO_UNIDADE VARCHAR(100),
CO_PROFISSIONAL_SUS VARCHAR(100),
NO_PROFISSIONAL VARCHAR(100),
CO_CBO	VARCHAR(30),
TP_SUS_NAO_SUS CHAR(1),
DS_ATIVIDADE_PROFISSIONAL VARCHAR(100),
NO_FANTASIA VARCHAR(100),
NO_BAIRRO VARCHAR(100),
NO_MUNICIPIO VARCHAR(100),
CO_MUNICIPIO INTEGER,
CO_SIGLA_ESTADO CHAR(2),
CO_CEP INTEGER,
DATA_INGESTAO DATE
);

CREATE TABLE PUBLIC.CURATED_SERVICOS(
CO_UNIDADE VARCHAR(100),
CO_SERVICO INTEGER,
CO_CLASSIFICACAO INTEGER,
DS_CLASSIFICACAO_SERVICO	VARCHAR(100),
DATA_INGESTAO DATE
);

CREATE TABLE PUBLIC.POPULACAO(
CO_MUNICIPIO INTEGER,
NO_MUNICIPIO VARCHAR(50),
POPULACAO INTEGER)