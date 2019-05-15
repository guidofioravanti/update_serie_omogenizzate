-- Questo script serve per creare lo schema e le tabelle su cui caricare i dati necessari per l'aggiornamento 
-- delle serie omogeneizzate 1961-2015. Di anno in anno vanno caricati i dati 2016-2018. L'anno successivo 
-- si cancellerà il tutto e si caricheranno i dati 2016-2019.
-- I dati che vanno caricati sono quelli che risultano dai controlli spaziali dopo aver applicato i flag di qualità: applicaFlag.R


CREATE SCHEMA IF NOT EXISTS update_serie_omogenee;
DROP TABLE IF EXISTS update_serie_omogenee.tmax_giornaliere_update;
DROP TABLE IF EXISTS update_serie_omogenee.tmin_giornaliere_update;

--Tmax
CREATE TABLE IF NOT EXISTS update_serie_omogenee.tmax_giornaliere_update(yymmdd date, cod_rete_guido smallint, siteid smallint,temp float);
ALTER TABLE update_serie_omogenee.tmax_giornaliere_update ADD PRIMARY KEY (yymmdd,cod_rete_guido,siteid); 
ALTER TABLE update_serie_omogenee.tmax_giornaliere_update ADD CONSTRAINT controllo_date CHECK(yymmdd>='2016-01-01' AND yymmdd <='2018-12-31'); 
ALTER TABLE update_serie_omogenee.tmax_giornaliere_update ADD CONSTRAINT controllo_valori CHECK(temp <=50 AND temp >= -25);
ALTER TABLE update_serie_omogenee.tmax_giornaliere_update ADD CONSTRAINT chiave_siteid_tmax FOREIGN KEY (cod_rete_guido,siteid) REFERENCES anagrafica.stazioni(cod_rete_guido,siteid) ON UPDATE CASCADE;   

--Tmin
CREATE TABLE IF NOT EXISTS update_serie_omogenee.tmin_giornaliere_update(yymmdd date, cod_rete_guido smallint, siteid smallint,temp float);
ALTER TABLE update_serie_omogenee.tmin_giornaliere_update ADD PRIMARY KEY (yymmdd,cod_rete_guido,siteid); 
ALTER TABLE update_serie_omogenee.tmin_giornaliere_update ADD CONSTRAINT controllo_date CHECK(yymmdd>='2016-01-01' AND yymmdd <='2018-12-31'); 
ALTER TABLE update_serie_omogenee.tmin_giornaliere_update ADD CONSTRAINT controllo_valori CHECK(temp <=50 AND temp >= -40);
ALTER TABLE update_serie_omogenee.tmin_giornaliere_update ADD CONSTRAINT chiave_siteid_tmin FOREIGN KEY (cod_rete_guido,siteid) REFERENCES anagrafica.stazioni(cod_rete_guido,siteid) ON UPDATE CASCADE;   
