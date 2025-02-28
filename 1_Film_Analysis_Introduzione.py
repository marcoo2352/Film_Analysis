#importiamo le librerie 
import polars as pl  
import streamlit as st
import altair as alt

Movies =  pl.read_csv("16k_Movies.csv") #importo il file csv
#################
# Puliamo il Dataset
#################
#print(Movies.select(pl.col("Duration")).to_series().to_list())
len(Movies.select(pl.col("Duration")).to_series().to_list())

#creiamo una funzione per convertire il tempo in minuti per facilità

def tempo_in_minuti(testo):
    if not testo:
        return "null"
    ore = 0
    minuti = 0
    if "h" in testo:
        parti = testo.split("h")
        try:
            ore = int(parti[0].strip())
        except ValueError:
            return "null"
    if "m" in testo:
        parti = testo.split("m")[0].strip()
        if "h" in parti:
            parti = parti.split("h")[1].strip()
        try:
            minuti = int(parti)
        except ValueError:
            if ore == 0:
                return "null"
    if ore > 0 or minuti > 0:
        return ore * 60 + minuti
    else:
        return "null"  

def generazione_lista_minuti(lista):
    n = len(lista)
    new_lista = [0] * n
    for i in range(0, n):
        new_lista[i] = tempo_in_minuti(lista[i])
    return new_lista
minuti = generazione_lista_minuti(Movies.select(pl.col("Duration")).to_series().to_list())

Movies = Movies.with_columns(
    pl.Series("Minutes", minuti, strict=False)
)
#print(Movies)


# Otteniamo la lista dei generi unici

generi = Movies.select(pl.col("Genres")).to_series().to_list()
generi_unici = {
    genere.strip()
    for item in generi if item is not None  # Filtra i None
    for genere in item.split(",")
}
#print(generi_unici)
#{'Romance', 'Sci-Fi', 'Adventure', 'News', 'Drama', 'Animation', 'Fantasy', 
# 'Mystery', 'Western', 'Reality-TV', 'Unknown', 'Family', 'Crime', 'Game-Show', 
# 'Thriller', 'History', 'Documentary', 'Action', 'Sport', 'Biography', 'Horror', 
# 'Musical', 'Comedy', 'Music', 'Talk-Show', 'War', 'Film-Noir'}



date = Movies.select(pl.col("Release Date")).to_series().to_list()
def ottieni_data(data):

        l = data.split(",")
        anno = int(l[1].strip())
        giorno = int(l[0].split(" ")[1].strip())
        mese = str(l[0].split(" ")[0])
        return (giorno, mese,anno)
def generazione_lista_data(lista):
    n = len(lista)
    anno = [0] * n
    mese = [0] * n
    giorno = [0] * n
    for i in range(0, n):
        anno[i] = ottieni_data(lista[i])[2]
        mese[i] = ottieni_data(lista[i])[1]
        giorno[i] = ottieni_data(lista[i])[0]
    return((anno, mese, giorno))



giorno = generazione_lista_data(date)[2]
#print(giorno)
Movies = Movies.with_columns(
    pl.Series("Day", giorno, strict=False)
)
#print(Movies)

mese = generazione_lista_data(date)[1]

Movies = Movies.with_columns(
    pl.Series("Month", mese, strict=False)
)
#print(Movies)

anno = generazione_lista_data(date)[0]

Movies = Movies.with_columns(
    pl.Series("Year", anno, strict=False)
)
#print(Movies)

result = Movies.with_columns(
    pl.col("Genres")
    .str.split(",")  # Dividi la stringa dei generi in una lista
    .list.len()      # Conta il numero di elementi nella lista
    .alias("num_genres")
)

# Trova il massimo numero di generi
max_genres = result.select(pl.max("num_genres")).item()

#print(f"Il numero massimo di generi assegnati a un film è: {max_genres}")
# Il numero massimo di generi assegnati a un film è: 9
#print(Movies.columns)
# Genre è nella nona colonna posizione della lista
import polars as pl

def assegnazione_generi(df):
    # Separa i generi in liste, gestendo i valori nulli o vuoti
    df = df.with_columns(
        pl.col(df.columns[9])
        .str.split(",").alias("GenreS")
    )
    
    # Esplodi la colonna dei generi in più righe
    df = df.explode("GenreS")
    
    # Sostituisci eventuali valori nulli con "null"
    df = df.with_columns(
        pl.col("GenreS").fill_null("null")
    )
    
    return df

Moviest = assegnazione_generi(Movies)

def assegnazione_scrittori(df):
    # Separa i generi in liste, gestendo i valori nulli o vuoti
    df = df.with_columns(
        pl.col(df.columns[7])
        .str.split(",").alias("Writer")
    )

    df = df.explode("Writer")

    df = df.with_columns(
        pl.col("Writer").fill_null("null")
    )
    
    return df

Moviest = assegnazione_scrittori(Moviest)
Moviest = Moviest.drop(["Written by", "Duration", "Genres"])
print(Moviest.columns)
##############################################################################

#########################################
# Analisi Esplorativa Iniziale          #
#########################################

st.markdown("<h1 style='font-size: 45px;'>Analisi Dei Film</h1", unsafe_allow_html=True)
st.markdown("""In questa analisi esamineremo un insieme di film considerando cinque aspetti principali:
             il genere, il rating assegnato, gli autori, la durata e l’anno di uscita.        
            L'obiettivo è individuare tendenze e caratteristiche ricorrenti,
             osservando come questi elementi influenzino il successo e la percezione dei film.
            Analizzeremo i generi più rappresentati, i punteggi ricevuti, il ruolo di registi e sceneggiatori,
            infine la durata e la sua possibile correlazione con il gradimento.
            Inoltre, prenderemo in considerazione l’anno di uscita per capire come
            le tendenze cinematografiche siano cambiate nel tempo.
            Questa analisi ci aiuterà a tracciare un quadro generale
            sui film e sulle loro principali caratteristiche e le loro relazioni.
""")


#prima introduzione sui dati

st.markdown("<h1 style='font-size: 40px;'>1. Visualizziamo il Campione</h1", unsafe_allow_html=True)
st.markdown("""In questo dataset stiamo utilizzando circa 16000 film, stiamo considerando un vasto numero di generi come:
            Romance, Sci-Fi, Adventure, Mystery, Drama, Animation, Fantasy... Stiamo prendendo in considerazione un intervallo 
            temporale cha va dal 1910 fino ad oggi, considerando film prodotti da una vasta gamma di registi.
            Scopriamo meglio il nostro campione

""")

st.markdown("<h1 style='font-size: 30px;'>1.1 Generi </h1", unsafe_allow_html=True)



pie_graph = (alt.Chart(Moviest)
    .mark_arc(
        cornerRadius=8,
        radius=120,
        radius2=80)
    .encode(
        alt.Theta("count(GenereS):Q"))
    .properties(height = 300, width = 600))


tot = (
    alt.Chart(Moviest)
    .mark_text(radius=0, size=30, color= "white")
    .encode(alt.Text("count(GenereS):Q"))   #conta il numero di elementi per ciascuna categoria
    .properties(height=300, width=600)
)

st.altair_chart(
    pie_graph + tot,
    use_container_width=True
)

