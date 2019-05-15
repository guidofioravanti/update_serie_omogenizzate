#15 maggio 2019
#
# Carichiamo i dati di output dei controlli spaziali sul database scia, nello schema "update_serie_omogenee". Il programma
# carica non tutte le serie giornaliere ma solo quelle che hanno una corrispondenza nello schema "serie_utili" e nella tabella "tmax/tmin_acmant_giornaliere"
#
# Avendo tutti i dati caricati nel db, possiamo utilizzarli per il calcolo delle anomalie ed estremi per il Rapporto Indicatori
# STIAMO TRATTANDO SOLO LE SERIE OMOGEE. LE SERIE DAL 1961-2015 OMOGENEIZZATE SI TROVANO NELLE TABELLE SOTTO GLI SCHEMI serie e serie_utili
# CON QUESTO PROGRAMMA CARICHIAMO I VALORI DAL 2016 FINO A OGGI.
######################################################################################
#ATTENZIONE: 
#
#1) il programma non richiede che venga specificato di volta in volta il nome della regione di cui dobbiamo caricare i dati sul db.
#   mediante la funzione "qualeRegione" il programma determina la regione di interesse. 
#   La funzione qualeRegione fa affidamento sul fatto che la directory si chiami spatial_controls_NOMEREGIONE_etc_etc dove NOMEREGIONE non deve seguire nessuno schema particolare (maiuscole, minuscole,spazi)..ci si affida ad "agrep"
#2) Lo script sql per creare le tabelle "tmax_giornaliere_update/tmin_giornaliere_update" deve essere stato eseguito prima di lanciare il programma.
#3) I dati vanno caricati sul database dal 2016 (i file di testo vengono letti e filtrati dal 2016 in poi)
#4) dbWiteTable usa l'opzione append=TRUE ma poichè cod_rete_guido,siteid,yymmdd costituiscono una PRIMARY KEY della tabella dove vengono caricati i dati
#   non corro il rischio di ricaricare due volte gli stessi dati lanciando per due volte il programma per la stessa regione 
######################################################################################

rm(list=objects())
library("RPostgreSQL")
library("stringr")
library("readr")
library("dplyr")
library("purrr")

######################################################################################
# Funzione per determinare la regione di cui vanno caricati i dati --------
# Il nome della regione riportato nel nome della directory di lavoro non necessariamente coincide
# con il nome della regione che utilizziamo per l'elaborazione dei dati. Inoltre abbiamo bisogno del codice rete

#INPUT: reti= dataframe restituito dalla query alla tabella rete_guido_lp in tbl_lookup

qualeRegione<-function(reti){
  
  unlist(str_split(getwd(),"/"))->lista
  #nome della directory di lavoro
  lista[length(lista)]->nomeDir
  unlist(str_split(nomeDir,"_"))->dir_tonkes
  agrep(dir_tonkes[3],reti$nome_rete)->riga
  if(!length(riga)) stop(glue::glue("{dir_tokens[3]} non trovato nella tabella rete_guido_lp, mi fermo!"))
  
  #restituisco la riga che identifica la regione il cui nome compare nel nome della directory di lavoro (spatial_controls_nomeregione...)  
  return(riga)
  
}#fine qualeRegione

######################################################################################   


######################################################################################   
qualiStazioni<-function(codRete,param){
  
  tolower(param)->param
  if(! param %in% c("tmax","tmin")) stop(glue::glue("Parametro {param} non riconosciuto"))
  
  query_stazioni<-glue::glue("SELECT DISTINCT(pag.siteid) FROM serie_utili.{param}_acmant_giornaliere pag WHERE pag.cod_rete_guido={codRete};")
  dbGetQuery(conn=myconn,statement = query_stazioni)->codiciStazioni
  
  codiciStazioni[[1]]
}#fine qualiStazioni
######################################################################################   



######################################################################################   
#legge i file di testo
#Input: codice (codice della stazione)
leggi<-function(siteid,param,codiceRete){
  
  tolower(param)->param
  if(! param %in% c("tmax","tmin")) stop(glue::glue("Parametro {param} non riconosciuto"))
  
  if(param=="tmax"){
    cols_only(year="i",month="i",day="i",tmax="d")->colonneTipo  
  }else{
    cols_only(year="i",month="i",day="i",tmin="d")->colonneTipo     
  }#fine if
  
  tryCatch({
    readr::read_delim(paste0(siteid,".txt"),delim=",",col_names = TRUE,col_types = colonneTipo ) %>%
      rename(yy=year,mm=month,dd=day) %>%
      filter(yy>=2016) %>%    
      mutate(mm=str_pad(mm,pad="0",side="left",width=2),dd=str_pad(dd,pad="0",side="left",width=2)) %>%
      tidyr::unite(col="yymmdd",yy,mm,dd,sep="-") %>%
      mutate(yymmdd=as.Date(yymmdd),cod_rete_guido=codiceRete,siteid=siteid) %>%
      dplyr::select(yymmdd,cod_rete_guido,siteid,everything())->dati
    
    names(dati)[4]<-"temp"
    dati
  },error=function(e){
    NULL
  })->out 
  
  out
  
}#fine leggi

######################################################################################   


dbDriver(drvName = "PostgreSQL")->driver
dbConnect(drv=driver,user="guido",host="localhost",port=5432,dbname="scia")->myconn

#interrogo il db per la tabella rete_guido_lp e trovo la riga della regione che mi interessa
dbGetQuery(conn=myconn,statement = "SELECT * FROM tbl_lookup.rete_guido_lp rgl;")->dfReti
qualeRegione(reti = dfReti)->riga

#cod_rete
dfReti[riga,]$cod_rete->cod_rete

#nome_rete
dfReti[riga,]$nome_rete->nome_rete

#attenzione: tmax,"tmin" debbono corrispondere ai nomi dei parametri riportati nei nomi delle tabelle nello schema "update_serie_omogenee"
purrr::walk(c("tmax","tmin"),.f=function(parametro){
  
  print(glue::glue("CARICO DATI PER PARAMETRO {parametro}, REGIONE {nome_rete}"))
  
  #A questo punto cerchiamo tutte le stazioni con "cod_rete" caricate sul db nello schema serie_utili per il parametro specifico
  qualiStazioni(codRete =cod_rete,param = parametro )->codiciStazioni
  
  if(!length(codiciStazioni)){print(glue::glue("Nessuna stazione da caricare per PARAMETRO {parametro}, REGIONE {nome_rete}")); return(NULL)}
  
  #leggiamo i file in codiciStazioni
  purrr::map(codiciStazioni,.f=leggi,param=parametro,codiceRete=cod_rete) %>%
    compact() %>% reduce(bind_rows)->dfTemp
  
  #carico i dati sul database
  dbWriteTable(conn=myconn,name=c("update_serie_omogenee",glue::glue("{parametro}_giornaliere_update")),value=dfTemp,append=TRUE,row.names=FALSE)

})#fine purrr::walk

dbDisconnect(myconn)
