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
######################## BARPLOT GENERI ###################################
st.markdown("<h1 style='font-size: 30px;'>1.1 Generi </h1", unsafe_allow_html=True)


Film_Generi_Unici= Moviest.unique(subset=['Title', 'GenreS'])


# Supponiamo che Moviest sia il tuo DataFrame polars
# Film_Generi_Unici = Moviest.unique(subset=['Title', 'GenreS'])

# Calcola il conteggio dei film per genere
Film_Generi_Count = Film_Generi_Unici.group_by('GenreS').agg(pl.count().alias('count'))

# Crea il grafico
genre_colors = {
    "Action": '#E63946',         # Vibrant red
    "Adventure": '#F9844A',      # Orange
    "Animation": '#F9C74F',      # Bright yellow
    "Biography": '#90BE6D',      # Green
    "Comedy": '#00FF00',         # Lime green (molto vivace per Comedy)
    "Crime": '#4A4E69',          # Dark blue-gray
    "Documentary": '#577590',    # Medium blue
    "Drama": '#0000FF',          # Blue (molto vivace per Drama)
    "Family": '#80FFDB',         # Mint
    "Fantasy": '#9B5DE5',        # Light purple
    "Film-Noir": '#14213D',      # Dark navy
    "Game-Show": '#FF9F1C',      # Golden orange
    "History": '#8A5A44',        # Brown
    "Horror": '#540B0E',         # Dark red
    "Music": '#43AA8B',          # Teal
    "Musical": '#F15BB5',        # Hot pink
    "Mystery": '#4E5283',        # Dark purple-blue
    "News": '#4CC9F0',           # Light blue
    "Reality-TV": '#F15025',     # Bright orange-red
    "Romance": '#FF00FF',        # Magenta (molto vivace per Romance)
    "Sci-Fi": '#2EC4B6',         # Turquoise
    "Sport": '#2D7DD2',          # Medium blue
    "Talk-Show": '#FFC6FF',      # Light pink
    "Thriller": '#FF0000',       # Red (molto vivace per Thriller)
    "Unknown": '#BCBCBC',        # Gray
    "War": '#606C38',            # Olive green
    "Western": '#DDA15E'         # Tan/sand
}
print(Film_Generi_Count)
bar_chart_generi = alt.Chart(Film_Generi_Count).mark_bar().encode(
    alt.Color("GenreS:N").scale(domain=list(genre_colors.keys()), range=list(genre_colors.values())).legend(None),
    alt.X("count:Q"),
    alt.Y("GenreS:N", sort='-x')
).properties(
    width=600,
    height=700
)



frequenze_generi = Film_Generi_Unici.group_by("GenreS").count().sort("count", descending=True)
dict_frequenze = dict()
for i in range(28):
    dict_frequenze[frequenze_generi.row(i)[0]] = frequenze_generi.row(i)[1]

col1, col2 = st.columns([1, 2])  # 1/3 testo, 2/3 grafico

# Testo nella prima colonna (sinistra)

with col1:
    st.write("### Distribuzione dei Generi")
    st.write("""
    Come possiamo vedere dal grafico nei 16000 film il genere maggiormente presente sono le commedy con , seguite dai thriller, romance e crime. Dovremo comunque stare attenti nelle analisi succesive nel valutare i generi meno rappresentati:
    """)
    frequenze_str = " | ".join(f"{key}: {value}" for key, value in dict_frequenze.items())
    st.markdown(f"{frequenze_str}")

# Grafico nella seconda colonna (destra)
with col2:
    st.altair_chart(bar_chart_generi, use_container_width=True)

####################    Serie Anni         #########################################
st.markdown("<h1 style='font-size: 30px;'>1.2 Anni </h1", unsafe_allow_html=True)


Film_Anni_Unici = Moviest.unique(subset=['Title', 'Year'])
 
Film_Anni_Count = Film_Generi_Unici.group_by('Year').agg(pl.count().alias('count')).sort('Year')
 
bar_chart_anno = alt.Chart(Film_Anni_Count).mark_bar().encode(
    alt.Y("count:Q", title="Numero di Film"),
    alt.X("Year:N", title="Anno", sort="x"),
    alt.Color('count:Q', scale=alt.Scale(scheme='blues'), legend=None)
).properties(
    width=600,
    height=600,
    title="Numero di Film Unici per Anno"
)



st.altair_chart(bar_chart_anno)
st.markdown(""" La quantità di film a nostra disposizione varia molto:
            fino al 1980 abbiamo circa un centinaio di film per anno, questa quantità 
            continua ad aumentare gradualmente fino a gli anni la quale impenna stabiliazzandosi intorno al 2005, 
            e con un leggero calo nel 2008 una possibile causa può essere la crisi globale. Tuttavia già degli anni successivi 
            si ritorna a un trend crescente...
             """)

################## Grafico a torta durata ############################
st.markdown("<h1 style='font-size: 30px;'>1.3 Durata </h1", unsafe_allow_html=True)


Film_Minuti_Unici = Moviest.unique(subset=['Title', 'Minutes'])



# Convertiamo i valori "null" in null di Polars
Film_Minuti_Unici = Film_Minuti_Unici.with_columns(
    pl.when(pl.col("Minutes") == "null")
      .then(None)  # Converti "null" in null
      .otherwise(pl.col("Minutes"))
      .cast(pl.Int32)  # Converti la colonna in tipo intero
      .alias('Minutes')
)

# Aggiungiamo una colonna con le classi dei minuti, gestendo i valori nulli
Film_Minuti_Unici = Film_Minuti_Unici.with_columns(
    pl.when(pl.col("Minutes").is_null())
      .then(pl.lit('Sconosciuto'))
      .when(pl.col("Minutes") <= 60)
      .then(pl.lit('<1h'))
      .when((pl.col("Minutes") > 60) & (pl.col("Minutes") <= 90))
      .then(pl.lit('1h-1.5h'))
      .when((pl.col("Minutes") > 90) & (pl.col("Minutes") <= 120))
      .then(pl.lit('1.5h-2h'))
      .when((pl.col("Minutes") > 120) & (pl.col("Minutes") <= 180))
      .then(pl.lit('2h-3h'))
      .otherwise(pl.lit('>3h'))
      .alias('Minute_Class')
)

# Raggruppiamo per le classi e contiamo i film
Film_Minuti_Count = Film_Minuti_Unici.group_by('Minute_Class').agg(
    pl.count().alias('count')
).sort('Minute_Class')

pie_graph = (
    alt.Chart(Film_Minuti_Count)
    .mark_arc(

        radius=120,
        stroke="#ffffff",  # Bordo bianco tra le fette
        strokeWidth=0.5
    )
    .encode(
        theta=alt.Theta("count:Q", title="Numero di film"),  # Usa il conteggio per la dimensione delle fette
        color=alt.Color("Minute_Class:N", title="Durata del film"),  # Usa la classe per il colore
        tooltip=["Minute_Class", "count"]  # Aggiungi un tooltip per visualizzare i dettagli
    )
    .properties(
        title="Distribuzione della durata dei film",
        height=400,
        width=600
    )
)

# Creazione del grafico a torta base
base = alt.Chart(Film_Minuti_Count)

# Strato principale per le fette
arc = base.mark_arc(
    radius=120,
    stroke="#ffffff",
    strokeWidth=0.5
).encode(
    theta=alt.Theta("count:Q", title="Numero di film"),
    color=alt.Color("Minute_Class:N", title="Durata del film"),
    tooltip=["Minute_Class", "count"]
)

# Strato per le percentuali
text = base.mark_text(
    align='center',
    baseline='middle',
    fontSize=12,
    color='black',
    radius=140  # Posiziona le etichette leggermente fuori dal bordo
).transform_joinaggregate(
    TotalCount='sum(count)'  # Calcola il totale
).transform_calculate(
    Percentuale="datum.count / datum.TotalCount * 100"  # Calcola la percentuale
).encode(
    theta=alt.Theta("count:Q"),  # Mantiene lo stesso allineamento delle fette
    text=alt.Text("Percentuale:Q", format=".1f%")  # Formato percentuale con un decimale
)

# Combinazione dei due strati
pie_graph = (arc + text).properties(
    title="Distribuzione della durata dei film",
    height=400,
    width=600
)

# Visualizzazione del grafico in Streamlit
st.altair_chart(pie_graph)
st.markdown(""" Possiamo notare che 
             """)
