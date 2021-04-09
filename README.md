# Data-Management-project
How Bitcoin shapes the  sentiment on Twitter

Due to the size of the file, the document concerning tweets from November is omitted.

Note per la configurazione
Prima di poter testare l’applicativo è 
necessario impostare alcuni parametri nel 
file Utilities.py:
1. is_realtime: True per eseguire la 
versione realtime; False altrimenti
2. analysis_thread[min=1; max=4] : 
Seleziona il numero di thread su cui 
dovrà essere effettuata l’analisi
Scraping Realtime:
3. chrome_binary_location: Percorso al file 
binary di Chrome
4. chromedriver_path : Percorso ai 
chromedriver
Scraping Non Realtime - Dati Storici:
5. path_to_btc_csv: Percorso alla tabella 
csv BTC
6. path_to_tweet_csv: Percorso alla tabella 
csv TWEET
7. path_to_tps_csv: Percorso alla tabella 
csv TPS
Nota: i percorsi nel caso in cui le 
tabelle sono posizionate nella 
cartella Producer sono già
impostati.
Istruzioni per la configurazione 
Si riportano le istruzioni per la
configurazione di pycharm.
1. Una volta aperto il progetto configurare 
l’interprete e scaricare le librerie
2 Bisogna aggiungere le 
configurazioni per l’esecuzione dei file 
***Script.py
3. Dovrebbero risultare quindi quattro 
configurazioni da poter eseguire:
• DataProcessScript.py
• DataStorageScript.py
• ProducerHD.py (Producer Hist. 
Data)
• RealTimeProducerScript.py
4. Una volta avviati i servizi di Apache e 
Mongo, l’avvio dell’applicativo 
dovrebbe avvenire come segue: 
1. DataStorageScript
2. ProducerHD o RealTime
3. DataProcessScript

