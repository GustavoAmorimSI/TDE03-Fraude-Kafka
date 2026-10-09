
CREATE TABLE IF NOT EXISTS transacoes (
    id SERIAL PRIMARY KEY,
    id_transacao VARCHAR(100) NOT NULL,
    id_cliente VARCHAR(100) NOT NULL,
    valor DECIMAL(10,2) NOT NULL,
    country VARCHAR(10) NOT NULL,
    city VARCHAR(100) NOT NULL,
    data_hora TIMESTAMP NOT NULL,
    fraude BOOLEAN NOT NULL DEFAULT FALSE,
    motivo VARCHAR(200)
);
