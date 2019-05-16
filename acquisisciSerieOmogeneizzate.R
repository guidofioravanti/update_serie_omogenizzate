#15 maggio 2019

############################################################
#Acquisiamo i dati omogeneizzati dal 1961 a oggi, ovvero:
# - serie 1961 - 2015 dalla tabella serie_utili (vanno filtrate per yymmdd <= 2015-12-31)
# - serie dal 2016 a oggi dallo schema "update_serie_omogenee"
############################################################
rm(list=objects())
library("RPostgres")
library("stringr")
library("readr")
library("dplyr")
library("purrr")
library("glue")

param<-c("tmax","tmin")[2] #SCEGLIERE UNO DEI DUE PARAMETRI

dbConnect(drv=RPostgres::Postgres(),user="guido",host="localhost",port=5432,dbname="scia")->myconn

#LA SEGUENTE QUERY ESTRAE I DATI DA SERIE_UTILI (CHE HANNO RMSE < 2) E LI INCROCIA CON I CODICI
#IN analisi.codici DOVE NON COMPAIONO LE STAZIONI TROPPO VICINE A QUELLE DELL'AERONAUTICA.

glue("WITH tabella AS (

select tag.yymmdd,tag.siteid,tag.cod_rete_guido,tag.temp from serie_utili.{param}_acmant_giornaliere tag 
where tag.yymmdd<='2015-12-31' AND  tag.cod_rete_guido||'_'||tag.siteid  IN (
  
  select cod_rete_guido||'_'||siteid FROM analisi.codici
  
) UNION 
    
select tgu.yymmdd,tgu.siteid,tgu.cod_rete_guido,tgu.temp from update_serie_omogenee.{param}_giornaliere_update tgu 
where  tgu.cod_rete_guido||'_'||tgu.siteid  IN (
  
  select cod_rete_guido||'_'||siteid FROM analisi.codici
  
)) -- ORA ACQUISISCO I NOMI DELLE REGIONI/RETI
SELECT yymmdd,rgl.nome_rete||'_'||tabella.siteid AS codice,temp FROM tabella 
  LEFT JOIN tbl_lookup.rete_guido_lp rgl ON rgl.cod_rete=tabella.cod_rete_guido 
  ORDER BY codice, yymmdd
")->queryRetrieveData

dbGetQuery(conn=myconn,statement = queryRetrieveData)->dati

dati %>%
  tidyr::spread(codice,temp) %>%
  tidyr::separate(col="yymmdd",into=c("yy","mm","dd"))->finale

dbDisconnect(myconn)

min(finale$yy)->annoi
max(finale$yy)->annof

seq.Date(from = as.Date(glue("{annoi}-01-01")),to=as.Date(glue("{annof}-12-31")),by="day")->calendario
if(length(calendario)!=nrow(finale)) stop("errore nel dataframe finale")

readr::write_delim(finale,path=glue("{param}_serie_omogeneizzate_{annoi}_{annof}.csv"),delim=";",col_names = TRUE)
