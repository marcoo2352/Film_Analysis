#importiamo le librerie 
import polars as pl  
import streamlit as st
import altair as alt
import numpy as np


Movies =  pl.read_csv("16k_Movies.csv") #importo il file csv
#################
# Puliamo il Datasetpip show statsforecast
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

def assegnazione_registi(df):
    # Separa i generi in liste, gestendo i valori nulli o vuoti
    df = df.with_columns(
        pl.col(df.columns[6])
        .str.split(",").alias("Director")
    )

    df = df.explode("Director")

    df = df.with_columns(
        pl.col("Director").fill_null("null")
    )
    
    return df
Moviest = assegnazione_registi(Moviest)
Moviest= Moviest.with_columns(
    (pl.col("Title") + " (" + pl.col("Year").cast(pl.Utf8) + ")").alias("Title")
)
Moviest = Moviest.drop(["Written by", "Duration"])
Moviest = Moviest.with_columns(Moviest["Director"].str.strip_chars())


##############################################################################

#########################################
# Analisi Esplorativa Iniziale          #
#########################################
st.set_page_config(layout="wide")
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
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

##################################################################à
######################## BARPLOT GENERI ###########################
###################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>1.1 Generi Cinematografici </h1", unsafe_allow_html=True)


Film_Generi_Unici= Moviest.unique(subset=['Title', 'GenreS'])



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


# Grafico 

highlight = alt.selection_single(
    on='mouseover',  # Attiva l'effetto al passaggio del mouse
    fields=['GenreS'],  # Campo su cui applicare l'effetto
    empty='none'  # Nessun effetto quando non c'è selezione
)

# 2. Creazione del bar chart con bordo condizionale
bar_chart = alt.Chart(Film_Generi_Count).mark_bar(
    cornerRadiusTopRight=5,
    cornerRadiusTopLeft=5,
    size=20,
    strokeWidth=2  # Spessore del bordo
).encode(
    x=alt.X('count:Q', title='Numero di Film'),
    y=alt.Y('GenreS:N', title='Genere', sort='-x'),
    color=alt.Color('GenreS:N').scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None),
    stroke=alt.condition(  # Condizione per il bordo
        highlight,
        alt.value('black'),  # Bordo nero quando evidenziato
        alt.value(None)  # Nessun bordo altrimenti
    ),
    opacity=alt.condition(  # Opacità per migliorare l'effetto
        highlight,
        alt.value(1),  # Pienamente visibile quando evidenziato
        alt.value(0.8)  # Leggermente trasparente altrimenti
    )
)

# 3. Aggiungi la selezione al grafico
bar_chart = bar_chart.add_selection(highlight)

# 4. Aggiungi etichette (come prima)
labels = alt.Chart(Film_Generi_Count).mark_text(
    align='left',
    baseline='middle',
    dx=3,
    fontSize=12,
    color='black'
).encode(
    x=alt.X('count:Q'),
    y=alt.Y('GenreS:N', sort='-x'),
    text=alt.Text('count:Q')
)

# 5. Combina grafico e etichette
combined_chart = (bar_chart + labels).properties(
    width=600,
    height=700,
    padding = {"right": 20}
).configure_axis(
    labelFontSize=12,
    titleFontSize=14
)

# 6. Mostra il grafico in Streamlit
col1, col2, col3, col4 = st.columns([1, 3, 5, 1])
# Testo nella prima colonna (sinistra)

with col2:
    st.write("### Distribuzione dei Generi")
    st.write("""
    Come possiamo vedere dal grafico nei 16000 film il genere maggiormente rappresentato è Drama per distacco, dopo troviamo Comedy, Thriller e Romance.
    Nelle nostre analisi dovremo porre attenzione ai generi: Film-Noir, Talk Show, Reality TV e Game Show; i quali hanno soltanto una osservazione quindi se trattati adeguatamente potrebbero distorcere le analisi.
    Nonostante queste singolarità gli altri generi sono ben rappresentati
    """)

# Grafico nella seconda colonna (destra)
with col3:
    st.altair_chart(combined_chart, use_container_width=True)

#########################################################################
####################    Serie Anni         ##############################
#########################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>1.2 Film per Anno e Genere</h1>", unsafe_allow_html=True)


@st.cache_data 

def load_data():
    return Moviest.unique(subset=['Title', 'Year', "GenreS"])

Film_Anni_Generi = load_data()
Film_Anni_Generi = (
    Film_Anni_Generi
    .group_by(pl.col("Year"), pl.col("GenreS"))
    .agg(Numero_Di_Film=pl.col("Title").count())
)
@st.cache_data
def somma_film_anno():
    Somma_anni = Moviest.unique(subset=['Title', 'Year'])
    Somma_anni = (Somma_anni.group_by(pl.col('Year'))
        .agg(Numero_Di_Film=pl.col("Title").count())    
    )
    return(Somma_anni)
Yearsum = somma_film_anno()
Yearsum = Yearsum.with_columns(pl.lit("Total").alias("GenreS"))

# Unisci i due dataset
Yearsum = Yearsum.select(["Year", "GenreS", "Numero_Di_Film"])
Film_Anni_Generi = pl.concat([Film_Anni_Generi, Yearsum])

genre_colors["Total"] = "black" 

# Ottieni tutti gli anni e generi unici
all_years = Film_Anni_Generi.select(pl.col("Year")).unique().sort("Year")
all_genres = Film_Anni_Generi.select(pl.col("GenreS")).unique()

# Crea un dataset completo con tutte le combinazioni di Year e GenreS
complete_data = all_years.join(all_genres, how="cross")


# Unisci i dati completi con i dati aggregati
Film_Anni_Generi_completo = complete_data.join(
    Film_Anni_Generi, on=["Year", "GenreS"], how="left"
).fill_null(0)



# Estrai tutti i generi unici
@st.cache_data 
def load_data2():
    return Film_Anni_Generi.select(pl.col("GenreS")).unique()["GenreS"]


unique_genres = load_data2()

if "selected_genres" not in st.session_state:
    st.session_state.selected_genres = ["Total", "Drama", "Comedy", "Thriller"]

# Creazione della selezione con valori predefiniti
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    selected_genres = st.multiselect(
    'Seleziona i generi da visualizzare', 
    unique_genres, 
    default=st.session_state.selected_genres
    )

# **Aggiorna lo stato solo se c'è una modifica**
if selected_genres != st.session_state.selected_genres:
    st.session_state.selected_genres = selected_genres

# Filtra i dati in base ai generi selezionati
filtered_data = Film_Anni_Generi_completo.filter(pl.col("GenreS").is_in(st.session_state.selected_genres))

# Crea un oggetto chart base
chart = (
    alt.Chart(filtered_data)
    .encode(
        color=alt.Color('GenreS:N').scale(
            domain=list(genre_colors.keys()),
            range=list(genre_colors.values())
        ).legend(None)
    )
)

# Disegna la linea
line = chart.mark_line().encode(
    x=alt.X("Year:O"),
    y=alt.Y("Numero_Di_Film:Q")
)

# Trova il massimo valore di Year per ogni genere
last_values = filtered_data.group_by("GenreS").agg(pl.col("Year").max().alias("LastYear"))

# Filtra il dataset per mantenere solo i valori dell'ultimo anno disponibile per ogni genere
last_points = filtered_data.join(last_values, left_on=["GenreS", "Year"], right_on=["GenreS", "LastYear"])

# Disegna i pallini alle estremità
circle = (
    alt.Chart(last_points)
    .mark_circle(size=60)
    .encode(
        x="Year:O",
        y="Numero_Di_Film:Q",
        color="GenreS:N"
    )
)

# Aggiungi le etichette
text = (
    alt.Chart(last_points)
    .mark_text(align="left", dx=4, fontSize=12)
    .encode(
        x="Year:O",
        y="Numero_Di_Film:Q",
        text="GenreS",
        color="GenreS:N"
    )
)

# Combina gli elementi
final_chart = line + circle + text
# Visualizzazione in Streamlit







col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.altair_chart(final_chart, use_container_width=True)
    st.info("**Nota: molti film hanno piu' generi associati, quindi sommando le varie quantità per ogni genere non si ottiene il totale**")
    st.markdown(""" Possiamo notare che 
             """)


######################################################################
################## Barplot durata ############################
######################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>1.3 Durata </h1", unsafe_allow_html=True)


Film_Minuti_Unici = Moviest.unique(subset=['Title', 'Minutes'])

# Filtriamo le rig he con "Minutes" == "null"
Riga_null = Film_Minuti_Unici.filter(Film_Minuti_Unici["Minutes"] == "null")
Riga_null = Riga_null.group_by("Minutes").agg(Numero_Di_Film=pl.col("Title").count())

# Filtriamo solo i valori numerici in "Minutes"
Film_Minuti_Count = Film_Minuti_Unici.filter(Film_Minuti_Unici["Minutes"] != "null")

# Convertiamo i valori numerici (ignorando gli errori)
Film_Minuti_Count = Film_Minuti_Count.with_columns(
    pl.col("Minutes").str.strip_chars().cast(pl.Int64, strict=False)  # Rimuove spazi e converte
)

# Raggruppiamo per "Minutes" e contiamo i film
Film_Minuti_Count = (
    Film_Minuti_Count.group_by("Minutes")
    .agg(Numero_Di_Film=pl.col("Title").count())
    .sort("Minutes")
)





highlight = alt.selection_single(
    on='mouseover',  # Attiva l'effetto al passaggio del mouse
    fields=['Minutes'],  # Campo su cui applicare l'effetto
    empty='none'  # Nessun effetto quando non c'è selezione
)

# 2. Creazione del bar chart con bordo condizionale
bar_chart = alt.Chart(Film_Minuti_Count).mark_bar(
    cornerRadiusTopRight=5,
    cornerRadiusTopLeft=5,
    size=5,
    strokeWidth=1  # Spessore del bordo
).encode(
    x=alt.X('Minutes:N', title='Minutaggio', sort='ascending' ),
    y=alt.Y('Numero_Di_Film:Q', title='Numero Di Film'),
    stroke=alt.condition(  # Condizione per il bordo
        highlight,
        alt.value('black'),  # Bordo nero quando evidenziato
        alt.value(None)  # Nessun bordo altrimenti
    ),
    color=alt.condition(  # Condizione per il colore
        highlight,
        alt.value('yellow'),  # Colore giallo quando il mouse è sopra
        alt.value('steelblue')  # Colore predefinito (puoi cambiarlo se preferisci)
    ),
    opacity=alt.condition(  # Opacità per migliorare l'effetto
        highlight,
        alt.value(1),  # Pienamente visibile quando evidenziato
        alt.value(0.8)  # Leggermente trasparente altrimenti
    )
).properties(
    title="Distribuzione del Numero di Film per Minutaggio",
    width=800,  # Larghezza del grafico in pixel
    height=400 
)

# 3. Aggiungi la selezione al grafico
bar_chart = bar_chart.add_selection(highlight)

col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.altair_chart(bar_chart)
    st.markdown(""" Possiamo notare che 
             """)


######################################################################
################## Barplot Voti ############################
######################################################################
# Layout per il titolo
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>1.4 Numero di Valutazioni</h1>", unsafe_allow_html=True)

# Rimuoviamo i duplicati basandoci su 'Title' e 'No of Persons Voted'
Film_Voti_Unici = Moviest.unique(subset=['Title', 'No of Persons Voted'])

# Filtriamo eliminando i valori "null" scritti a parole
Film_Voti_Count = Film_Voti_Unici.filter(
    (pl.col("No of Persons Voted").is_not_null()) &  # Esclude i veri NULL
    (pl.col("No of Persons Voted") != "null")  # Esclude la stringa "null"
)

# Convertiamo la colonna in numerico
Film_Voti_Count = Film_Voti_Count.with_columns(
    pl.col("No of Persons Voted").str.strip_chars().cast(pl.Int64, strict=False)
)

# Creiamo due colonne: una per la visualizzazione e una per l'ordinamento
Film_Voti_Count = Film_Voti_Count.with_columns(
    pl.when(pl.col("No of Persons Voted") > 150)
    .then(pl.lit("150+"))  # Assegniamo il valore "200+" ai numeri sopra 200
    .otherwise(pl.col("No of Persons Voted").cast(pl.Utf8))
    .alias("Voti_Display"),  # Colonna per la visualizzazione con nome diverso

    pl.when(pl.col("No of Persons Voted") > 150)
    .then(pl.lit(151))  # Per l'ordinamento assegniamo 201 a "200+"
    .otherwise(pl.col("No of Persons Voted"))
    .alias("Sort_Order")  # Colonna numerica per ordinamento
)

# Raggruppiamo per "Voti_Display" e contiamo i film
Film_Voti_Count = (
    Film_Voti_Count.group_by(["Voti_Display", "Sort_Order"])
    .agg(Numero_Di_Film=pl.col("Title").count())
    .sort("Sort_Order")  # Ordiniamo in base alla colonna numerica
)

# Selezione interattiva per evidenziare barre nel grafico
highlight = alt.selection_single(
    on='mouseover',
    fields=['Voti_Display'],
    empty='none'
)

# Creiamo il grafico con etichette sull'asse x mostrate solo per valori specifici
bar_chart = alt.Chart(Film_Voti_Count).mark_bar(
    cornerRadiusTopRight=5,
    cornerRadiusTopLeft=5,
    size=5,
    strokeWidth=1
).encode(
    x=alt.X('Voti_Display:N', 
            title='N. Valutazioni', 
            sort=None,
            axis=alt.Axis(
                labelAngle=0,
                values=["1", "5", "10", "15", "20", "25", "30", "40", "50", "60", "70", "80", "90", "100", "150+"],
            )),
    y=alt.Y('Numero_Di_Film:Q', title='Numero Di Film'),
    stroke=alt.condition(highlight, alt.value('black'), alt.value(None)),
    color=alt.condition(highlight, alt.value('yellow'), alt.value('steelblue')),
    opacity=alt.condition(highlight, alt.value(1), alt.value(0.8)),
    tooltip=['Voti_Display', 'Numero_Di_Film']
).properties(
    title="Distribuzione del Numero di Valutazioni per Film",
    width=800,
    height=400
)

# Aggiungiamo la selezione al grafico
bar_chart = bar_chart.add_selection(highlight)

# Layout per il grafico
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.altair_chart(bar_chart)
    st.markdown("""
    Possiamo notare che i film con più di 200 valutazioni sono stati raggruppati in "200+" 
    per rendere il grafico più leggibile. Passa il mouse sulle barre per vedere i valori esatti.
    """)



######################################################################
################## Barplot Rating ############################
######################################################################
# First issue: The 'bar_chart' variable is used before being defined
# Second issue: Missing parentheses in the markdown HTML tag
# Third issue: Streamlit and Polars syntax mixed with undefined variables
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>1.5 Rating </h1>", unsafe_allow_html=True)  

# Supponiamo che "Moviest" sia un DataFrame di Polars
Film_Rating_Unici = Moviest.unique(subset=['Title', 'Rating'])  


Film_Rating_Count = Film_Rating_Unici.filter(pl.col("Rating").is_not_null())  

Film_Rating_Count = Film_Rating_Count.group_by("Rating").agg(
    pl.col("Rating").count().alias("Numero_Di_Film")
).sort("Rating")

Film_Rating_Count = Film_Rating_Count.with_columns(
    pl.col("Rating").cast(pl.Utf8)  # Convertiamo in stringa se non lo è già
)

Film_Rating_Count = Film_Rating_Count.filter(pl.col("Rating").is_not_null())





# Creiamo la selezione evidenziata
highlight = alt.selection_single(
    on='mouseover',  
    fields=['Rating'],  
    empty='none'  
)

# Creiamo il grafico a barre
bar_chart2 = alt.Chart(Film_Rating_Count).mark_bar(  
    cornerRadiusTopRight=5,
    cornerRadiusTopLeft=5,
    size=10,
    strokeWidth=3  
).encode(
    x=alt.X('Rating:N', title='Rating', sort='ascending'),
    y=alt.Y('Numero_Di_Film:Q', title='Numero Di Film'),
    stroke=alt.condition(
        highlight,
        alt.value('black'),  
        alt.value(None)  
    ),
    color=alt.condition(
        highlight,
        alt.value('yellow'),  
        alt.value('steelblue')  
    ),
    opacity=alt.condition(
        highlight,
        alt.value(1),  
        alt.value(0.8)  
    )
).properties(
    title="Distribuzione del Numero di Film per Rating",
    width=800,  
    height=400
).add_selection(highlight)  

# Visualizziamo il grafico con Streamlit
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.altair_chart(bar_chart2)
    st.markdown(""" Possiamo notare che """)


############################################################################àà
##############################################################################
###############################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 40px;'>2 Analisi dei Dati</h1", unsafe_allow_html=True)
    st.markdown("""L’analisi dei dati cinematografici si concentrerà su diversi aspetti chiave per comprendere l’evoluzione del cinema nel tempo. In particolare, verranno esaminate le variazioni nei generi cinematografici, l’impatto dei registi più influenti, la relazione tra durata dei film e rating, nonché le tendenze generali dell’apprezzamento del pubblico.
    Per questo studio, utilizzeremo informazioni dettagliate su ciascun film, inclusi il genere, il regista, la durata e il rating. Analizzeremo come questi fattori si siano modificati nel tempo, cercando di individuare pattern significativi e possibili correlazioni. Inoltre, studieremo l’eventuale affermazione di nuovi generi o la decadenza di altri, osservando come le preferenze del pubblico siano cambiate nel corso degli anni.
    L’obiettivo è fornire un quadro quantitativo e qualitativo dell’evoluzione del cinema, sfruttando i dati per supportare ipotesi e trarre conclusioni significative.
    """)
    st.markdown("<h1 style='font-size: 30px;'>2.1 Visualizzazione del rating rispetto a i generi</h1", unsafe_allow_html=True)

#################################################################################à
################# boxplot media voti generi######################################
##################################################################################

Film_Anni_Generi = Moviest.unique(subset=['Title', 'Year', "GenreS"])

Film_Anni_Generi2 = (
    Film_Anni_Generi.filter(
        (pl.col("Year").is_not_null()) & 
        (pl.col("GenreS").is_not_null()) & 
        (pl.col("GenreS") != "null")  # Rimosso il confronto con "null" su Year
    )
)
#{'Romance', 'Sci-Fi', 'Adventure', 'News', 'Drama', 'Animation', 'Fantasy', 
# 'Mystery', 'Western', 'Reality-TV', 'Unknown', 'Family', 'Crime', 'Game-Show', 
# 'Thriller', 'History', 'Documentary', 'Action', 'Sport', 'Biography', 'Horror', 
# 'Musical', 'Comedy', 'Music', 'Talk-Show', 'War', 'Film-Noir'}

Film_Anni_Generi2 = Film_Anni_Generi2.filter(~pl.col("GenreS").is_in(['Game-Show', 'Reality-TV', 'Talk-Show', 'Film-Noir', 'null', 'Unknown']))

order = (
    Film_Anni_Generi2.group_by("GenreS") # Raggruppa i dati per la variabile di interesse
    .agg(pl.median("Rating").alias("Median")) # Calcola la mediana
    .sort(["Median", "GenreS"], descending = True) # Ordina prima per mediana appena calcolata, poi alfabeticamente (in quanto ci sono più generi che hanno mediana uguale)
    .select("GenreS") # Seleziona solo i generi
    .to_series() # Converte in una serie
    .to_list() # Converte in una lista
) 


Rating_boxplot = (
    alt.Chart(Film_Anni_Generi2)  
    .mark_boxplot()
    .encode(
        x=alt.X("GenreS:N", title="Genere Di Film", sort=order),  # L'asse x rappresenta i generi musicali preferiti, ordinati secondo `order`
        y=alt.Y("Rating:Q", title="Valutazione Media Dei Film"),  # L'asse y rappresenta le ore medie di ascolto al giorno - VIRGOLA AGGIUNTA QUI
        color=alt.Color('GenreS:N').scale(
            domain=list(genre_colors.keys()),
            range=list(genre_colors.values())
        ).legend(None)
    )    
    .properties(
        title="Distribuzione del Rating per ogni Genere",  # Titolo del grafico
        height=500  # Altezza del grafico
    )
    .configure_title(fontSize=20)  # Imposta una dimensione maggiore per il titolo del grafico
)
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.altair_chart(Rating_boxplot, use_container_width=True)

######################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>2.2 Evoluzione del rating medio nel Tempo </h1", unsafe_allow_html=True)
Film_Anni_Generi3 =( Film_Anni_Generi.group_by( "GenreS", 'Year')
.agg(Numero_Di_Film=pl.col("Title").count(), Media_Voto =pl.col("Rating").mean())
)
Film_Anni_Generi3 = Film_Anni_Generi3.filter(pl.col("Media_Voto").is_not_null())

Yearsum = Film_Anni_Generi3.group_by("Year").agg(
    Numero_Di_Film=pl.col("Numero_Di_Film").sum(),
    Media_Voto=pl.col("Media_Voto").mean()
).with_columns(pl.lit("Total").alias("GenreS"))

Yearsum = Yearsum.select(["GenreS", "Year", "Numero_Di_Film", "Media_Voto"])
Film_Anni_Generi3 = pl.concat([Film_Anni_Generi3, Yearsum])




Film_Anni_Generi_completo2 = complete_data.join(
    Film_Anni_Generi3, on=["Year", "GenreS"], how="left"
).fill_null(0)

@st.cache_data 
def load_data10():
    return Film_Anni_Generi_completo2.select(pl.col("GenreS")).unique()["GenreS"]


unique_genres = load_data10()

if "selected_genres2" not in st.session_state:
    st.session_state.selected_genres2 = ["Total", "Drama", "Comedy", "Thriller"]

# Creazione della selezione con valori predefiniti
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    selected_genres2 = st.multiselect(
    'Seleziona i generi da visualizzare', 
    unique_genres, 
    default=st.session_state.selected_genres2
    )

# **Aggiorna lo stato solo se c'è una modifica**
if selected_genres2 != st.session_state.selected_genres2:
    st.session_state.selected_genres2 = selected_genres2

# Filtra i dati in base ai generi selezionati
filtered_data = Film_Anni_Generi_completo2.filter(pl.col("GenreS").is_in(st.session_state.selected_genres2))

# Crea un oggetto chart base
chart = (
    alt.Chart(filtered_data)
    .encode(
        color=alt.Color('GenreS:N').scale(
            domain=list(genre_colors.keys()),
            range=list(genre_colors.values())
        ).legend(None)
    )
)

# Disegna la linea
line = chart.mark_line().encode(
    x=alt.X("Year:O"),
    y=alt.Y("Media_Voto:Q")
)

# Trova il massimo valore di Year per ogni genere
last_values = filtered_data.group_by("GenreS").agg(pl.col("Year").max().alias("LastYear"))

# Filtra il dataset per mantenere solo i valori dell'ultimo anno disponibile per ogni genere
last_points = filtered_data.join(last_values, left_on=["GenreS", "Year"], right_on=["GenreS", "LastYear"])

# Disegna i pallini alle estremità
circle = (
    alt.Chart(last_points)
    .mark_circle(size=60)
    .encode(
        x="Year:O",
        y="Media_Voto:Q",
        color="GenreS:N"
    )
)

# Aggiungi le etichette
text = (
    alt.Chart(last_points)
    .mark_text(align="left", dx=4, fontSize=12)
    .encode(
        x="Year:O",
        y="Media_Voto:Q",
        text="GenreS",
        color="GenreS:N"
    )
)

# Combina gli elementi
final_chart = line + circle + text
# Visualizzazione in Streamlit

col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.altair_chart(final_chart, use_container_width=True)


##########################à##############################################################################
########################### Modello Arima ###############################################################
#########################################################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>2.2.1 Adattamente Modello Arima</h1", unsafe_allow_html=True)




##########################à##############################################################################
########################### Durata Rating ###############################################################
#########################################################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>2.3 Confronto tra Durata e Rating </h1", unsafe_allow_html=True)


# Your existing Polars data preparation
# Assuming Moviest is already defined
Film_Ore_Rating = Moviest.unique(subset=['Title'])
Film_Ore_Rating = Film_Ore_Rating.filter(
    pl.col("Rating").is_not_null() & (pl.col("Minutes") != "null")
)
Film_Ore_Rating1 = Film_Ore_Rating.with_columns(pl.col("Minutes").cast(pl.Float64))
Film_Ore_Rating = Film_Ore_Rating1.filter(pl.col("Minutes") <= 350)
# Convert to pandas for Altair compatibility
# Altair works better with pandas DataFrames
film_df = Film_Ore_Rating.to_pandas()

#


# Crea uno slider Streamlit
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    valore_soglia = st.slider("Soglia (Minuti)", min_value=0, max_value=350, value=5, step=1)

# Definisci il grafico base con il dataset
base = alt.Chart(film_df).properties(
    width=800,
    height=600
)

# Crea i livelli di visualizzazione
grafico_dispersione = base.mark_circle().encode(
    x=alt.X("Minutes:Q", scale=alt.Scale(domain=[0, 350])).title("Minuti"),
    y=alt.Y("Rating:Q").title("Rating")
).transform_filter(
    alt.datum.Minutes >= valore_soglia
)

grafico_bin = base.mark_circle().encode(
    x=alt.X("Minutes:Q", scale=alt.Scale(domain=[0, 350])).bin(maxbins=10),
    y=alt.Y("Rating:Q").bin(maxbins=10),
    size=alt.Size("count():Q").scale(domain=[0, 2000])
).transform_filter(
    alt.datum.Minutes < valore_soglia
)

# Crea una linea che copre l'intera altezza del grafico
linea = alt.Chart(pl.DataFrame({'x': [valore_soglia]})).mark_rule(color="red").encode(
    x='x:Q',
    size=alt.value(2)
).properties(
    height=600
)

# Combina tutti i livelli
grafico = alt.layer(grafico_dispersione, grafico_bin, linea)

# Visualizza il grafico in Streamlit
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.altair_chart(grafico, use_container_width=True)

correlation = Film_Ore_Rating1.select(pl.corr("Minutes", "Rating")).to_series()[0]
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.write("Correlazione:", correlation)



##########################à##############################################################################
########################### Durata Rating ###############################################################
#########################################################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>2.4 Confronto tra Durata e Periodo</h1", unsafe_allow_html=True)







##################################################################à
######################## Analisi FIlm Dettaglio ###################
###################################################################



col1, col2, col3 = st.columns([1, 8, 1])
with col2:

    st.markdown("<h1 style='font-size: 40px;'>3 Film nel dettaglio</h1", unsafe_allow_html=True)
    st.markdown("""In questa sezione ci occuperemo di adattare dei modelli per studiare meglio 

    """)

###################################################################
########### Grafico film per ogni decade ##########################
###################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>3.1 Film Migliori per ogni Genere e Decade</h1", unsafe_allow_html=True)




Film_agg = Moviest.unique(subset=["Title", "Rating", "Year"])
Film_agg = Film_agg.select(["Title", "Rating", "Year"])
Film_agg = Film_agg.with_columns(pl.lit("General").alias("GenreS"))
#
Film_rank = Moviest.unique(subset=["Title", "Year", "GenreS"])
Film_rank = Film_rank.select(["Title", "Rating", "Year", "GenreS"])
Film_rank = pl.concat([Film_rank, Film_agg])
Film_rank = Film_rank .drop_nulls("Rating")  # Rimuove righe dove "NomeColonna" è null
def get_decade(year):
    if year < 1980:
        return '1970-1979'
    elif year < 1990:
        return '1980-1989'
    elif year < 2000:
        return '1990-1999'
    elif year < 2010:
        return '2000-2009'
    elif year < 2020:
        return '2010-2019'
    else:
        return '2020+'

Film_rank = Film_rank.with_columns(
    Decade = pl.col("Year").map_elements(get_decade)
)
#
#######################
if "selected_genres3" not in st.session_state:
    st.session_state.selected_genres3 = "General"

@st.cache_data 
def load_data100():
    return Film_rank.select(pl.col("GenreS")).unique()["GenreS"]

unique_genres2 = load_data100()
unique_genres2_list = unique_genres2.to_list()

# Implementazione selectbox per i generi (singola selezione)
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    selected_genres3 = st.selectbox(
    "Seleziona un genere:",
    options=unique_genres2_list,
    index=unique_genres2_list.index("General") if "General" in unique_genres2_list else 0
    )

# Aggiorna lo stato solo se c'è una modifica
if selected_genres3 != st.session_state.selected_genres3:
    st.session_state.selected_genres3 = selected_genres3

# Filtra i dati in base al genere selezionato
Film_rank = Film_rank.filter(pl.col("GenreS") == st.session_state.selected_genres3)

### Dobbiamo selezionare 

def ranker(df):
    top_titles = []
    temp_df = df.clone()  # Crea una copia del DataFrame per evitare modifiche indesiderate

    while len(top_titles) < 3 and not temp_df.is_empty():
        max_value = temp_df["Rating"].max()  # Trova il valore massimo
        df_max = temp_df.filter(pl.col("Rating") == max_value)
        
        # Prendi il primo titolo tra quelli con il rating massimo
        title_to_add = df_max["Title"].to_list()[0]
        top_titles.append(title_to_add)
        
        # Rimuovi il titolo appena aggiunto
        temp_df = temp_df.filter(pl.col("Title") != title_to_add)

    return top_titles[:3]

def rankdecade(df):
    decadi = ('1970-1979', '1980-1989', '1990-1999', '2000-2009', '2010-2019', '2020+')
    top_per_decade = {}

    for d in decadi:
        sel = df.filter(pl.col("Decade") == d)
        top_per_decade[d] = ranker(sel)  # Associa la decade ai top 3 film
    return top_per_decade  # Restituisce un dizionario
top_film = rankdecade(Film_rank)


top_titles = [title for titles in top_film.values() for title in titles]

# Filtra il DataFrame originale per includere solo i top film
filtered_df = Film_rank.filter(pl.col("Title").is_in(top_titles))

# Mostra il DataFrame filtrato

# Creazione del grafico
df_1970 = filtered_df.filter((filtered_df['Decade'] == '1970-1979'))
df_1980 = filtered_df.filter((filtered_df['Decade'] == '1980-1989'))
df_1990 = filtered_df.filter((filtered_df['Decade'] == '1990-1999'))
df_2000 = filtered_df.filter((filtered_df['Decade'] == '2000-2009'))
df_2010 = filtered_df.filter((filtered_df['Decade'] == '2010-2019'))
df_2020 = filtered_df.filter((filtered_df['Decade'] == '2020+'))

genre_colors["General"] = "black"

# Then create separate charts for each decade
chart_1970 = alt.Chart(df_1970).mark_bar().encode(
    x=alt.X('Rating:Q', title='Rating'),
    y=alt.Y('Title:N', title='Film', sort='-x'),
    color=alt.Color('GenreS:N').scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None)
).properties(
    title="Top Films 1970-1979"
)

chart_1980 = alt.Chart(df_1980).mark_bar().encode(
    x=alt.X('Rating:Q', title='Rating'),
    y=alt.Y('Title:N', title='Film', sort='-x'),
    color=alt.Color('GenreS:N').scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None)
).properties(
    title="Top Films 1980-1989"
)

chart_1990 = alt.Chart(df_1990).mark_bar().encode(
    x=alt.X('Rating:Q', title='Rating'),
    y=alt.Y('Title:N', title='Film', sort='-x'),
    color=alt.Color('GenreS:N').scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None)
).properties(
    title="Top Films 1990-1999"
)

chart_2000 = alt.Chart(df_2000).mark_bar().encode(
    x=alt.X('Rating:Q', title='Rating'),
    y=alt.Y('Title:N', title='Film', sort='-x'),
    color=alt.Color('GenreS:N').scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None)
).properties(
    title="Top Films 2000-2009"
)

chart_2010 = alt.Chart(df_2010).mark_bar().encode(
    x=alt.X('Rating:Q', title='Rating'),
    y=alt.Y('Title:N', title='Film', sort='-x'),
    color=alt.Color('GenreS:N').scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None)
).properties(
    title="Top Films 2010-2019"
)

chart_2020 = alt.Chart(df_2020).mark_bar().encode(
    x=alt.X('Rating:Q', title='Rating'),
    y=alt.Y('Title:N', title='Film', sort='-x'),
    color=alt.Color('GenreS:N').scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None)
).properties(
    title="Top Films 2020+"
)

# Combine the charts vertically
final_chart1 = alt.vconcat(chart_1970, chart_1980,chart_1990,).configure_view(
    continuousWidth=150,
    continuousHeight=100
)

final_chart2 = alt.vconcat(chart_2000,chart_2010, chart_2020).configure_view(
    continuousWidth=150,
    continuousHeight=100
)

col1, col2, col3, col4 = st.columns([1,4,4, 1])
with col2:
    st.altair_chart(final_chart1)
with col3:
    st.altair_chart(final_chart2)

##################################################################
##################################################################
##################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>3.2 Migliori Registi dato il Rating</h1", unsafe_allow_html=True)


# Soluzione con hash_funcs per i DataFrame Polars
@st.cache_data(hash_funcs={"polars.dataframe.frame.DataFrame": lambda df: df.shape})
def get_unique_movies_by_director(dataset):
    unique_movies = dataset.unique(subset=["Title", "Director"])
    return unique_movies.filter(pl.col('Rating').is_not_null())

# Use the function immediately
Film_Registi_Unici = get_unique_movies_by_director(Moviest)

# Aggiungiamo hash_funcs anche a questa funzione
@st.cache_data(hash_funcs={"polars.dataframe.frame.DataFrame": lambda df: df.shape})
def calculate_director_averages(dataset):
    return dataset.group_by('Director').agg(
        pl.col('Rating').mean().alias('Voto_Medio'), 
        pl.col('Title').count().alias('Numero_Film')
    )

# Use the function immediately
Registi_Medie = calculate_director_averages(Film_Registi_Unici)

# Get minimum number of films input
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    number = st.number_input("Numero di film minimo per Regista", step=1, format="%d")

# Anche qui aggiungiamo hash_funcs
@st.cache_data(hash_funcs={"polars.dataframe.frame.DataFrame": lambda df: df.shape})
def filter_directors_by_film_count(dataset, min_films):
    return dataset.filter(pl.col('Numero_Film') >= min_films)

# Use the function immediately
Registi_Medie = filter_directors_by_film_count(Registi_Medie, number)

# Per questa funzione non è necessario il cache perché non prende DataFrame come input
def best_director(df):
    top_director = []
    dir_df = df.clone()
    
    while len(top_director) < 10 and not dir_df.is_empty():
        max_value = dir_df["Voto_Medio"].max()  # Find the maximum value
        df_max = dir_df.filter(pl.col("Voto_Medio") == max_value)
        
        # Extract directors with maximum rating
        new_directors = df_max["Director"].to_list()
        top_director.extend(new_directors)
        
        # Remove all directors just added
        dir_df = dir_df.filter(~pl.col("Director").is_in(new_directors))
    
    return top_director[:10]

# Use the function immediately
best_dire = best_director(Registi_Medie)

# Aggiungiamo hash_funcs a questa funzione
@st.cache_data(hash_funcs={"polars.dataframe.frame.DataFrame": lambda df: df.shape})
def filter_top_directors(dataset, director_list):
    return dataset.filter(pl.col("Director").is_in(director_list))

# Use the function immediately
df_best_dire = filter_top_directors(Registi_Medie, best_dire)

# Modifica seguendo il suggerimento dell'errore: aggiunta del carattere underscore

def create_director_bar_chart(_dataset):
    return alt.Chart(_dataset).mark_bar().encode(
        x=alt.X('Voto_Medio:Q', scale=alt.Scale(domain=[0, 10])),
        y=alt.Y("Director:N", sort='-x'),
    ).properties(height=400)

# Use the function immediately
Dir_Chart = create_director_bar_chart(df_best_dire)

col1, col2, col3, col4 = st.columns([1, 5, 3, 1])
with col2:
    st.altair_chart(Dir_Chart)

# Display top directors data
with col3:
    st.write(df_best_dire.sort("Voto_Medio", descending = True))

# Initialize session state if needed
if "selected_Director" not in st.session_state:
    st.session_state.selected_Director = None

# Get director list for selection
Registi_selection_box = df_best_dire.get_column("Director").to_list()

# Implement selectbox for directors
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    selected_Director = st.selectbox(
    "Seleziona il Regista:",
    Registi_selection_box,
    index=None)

# Update state only if there's a change
if selected_Director != st.session_state.selected_Director:
    st.session_state.selected_Director = selected_Director

# Aggiungiamo hash_funcs a questa funzione
@st.cache_data(hash_funcs={"polars.dataframe.frame.DataFrame": lambda df: df.shape})
def get_movies_by_director(dataset, director):
    unique_movies = dataset.unique(subset=["Title", "Director"])
    return unique_movies.filter(pl.col("Director") == director)

# Use the function immediately if a director is selected
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    if st.session_state.selected_Director:
        Selected_Dire = get_movies_by_director(Moviest, st.session_state.selected_Director)
        st.write(Selected_Dire.select(["Title", "Genres", "Minutes", "Rating", "No of Persons Voted"]))


##################################################################
############# Algoritmo di ricerca ###############################
##################################################################
# Filtraggio unico dei film per Titolo, Generi e Regista
# Filtraggio unico dei film per Titolo, Generi e Regista
# Filtraggio unico dei film per Titolo, Generi e Regista
Film_filtraggio_finale = Moviest.unique(subset=["Title", "GenreS", "Director"])

def get_durata(minuti):
    try:
        minuti_int = int(minuti)
    except (ValueError, TypeError):
        return 'Unknown'
    
    if minuti_int < 60:
        return '<60'
    elif minuti_int < 90:
        return '60-89'
    elif minuti_int < 120:
        return '90-119'
    elif minuti_int < 150:
        return '120-149'
    else:
        return '150=<'

@st.cache_data
def load_data1000():
    return Film_filtraggio_finale.select(pl.col("GenreS")).unique()["GenreS"]

generi_unici_filtro = load_data1000().to_list()

col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>3.3 Ricerca Dei Film</h1>", unsafe_allow_html=True)

    Title_sel = st.text_input("Titolo", None)
    
    if "filtro_generi" not in st.session_state:
        st.session_state.filtro_generi = []
    
    Filtro_generi = st.multiselect(
        'Seleziona i generi da visualizzare', 
        generi_unici_filtro, 
        default=st.session_state.filtro_generi
    )
    
    st.session_state.filtro_generi = Filtro_generi

Film_aggregati = Film_filtraggio_finale.group_by(["Title", "Director"]).agg(
    pl.col("GenreS").alias("Generi").unique(),
    pl.col("Year").first().alias("Year"),
    pl.col("Rating").first().alias("Avg_Rating"),
    pl.col("Minutes").first().alias("Minutes"), 
    pl.col("Description").first().alias("Description"),
    pl.col("Directed by").first().alias("Directed by"),
)

Film_aggregati = Film_aggregati.with_columns(
    Decade = pl.col("Year").map_elements(get_decade),
    Durata_Classe = pl.col("Minutes").map_elements(get_durata)
)

if Title_sel:
    Film_aggregati = Film_aggregati.filter(pl.col("Title").str.contains(Title_sel, literal=True))

if Filtro_generi:
    mask = Film_aggregati["Generi"].list.contains(Filtro_generi[0])
    for genere in Filtro_generi[1:]:
        mask &= Film_aggregati["Generi"].list.contains(genere)
    filtered_data_finale = Film_aggregati.filter(mask)
else:
    filtered_data_finale = Film_aggregati

col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    Decade_finale = st.selectbox(
        "Selezionare la Decade",
        ('1970-1979', '1980-1989', '1990-1999', '2000-2009', '2010-2019', '2020+'),
        index=None,
    )
if Decade_finale:
    filtered_data_finale = filtered_data_finale.filter(pl.col("Decade") == Decade_finale)

col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    Director_sel = st.text_input("Regista", None)
if Director_sel:
    filtered_data_finale = filtered_data_finale.filter(pl.col("Director").str.contains(Director_sel, literal=True, strict=False))

col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    Minuti_finale = st.selectbox(
        "Selezionare Minutaggio",
        ('Unknown', '<60', '60-89', '90-119', '120-149', '150=<'),
        index=None,
    )
if Minuti_finale:
    filtered_data_finale = filtered_data_finale.filter(pl.col("Durata_Classe") == Minuti_finale)

col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    keywords_input = st.text_input("Inserisci parole chiave (separate da virgole):")

if keywords_input:
    keywords_list = [k.strip().lower() for k in keywords_input.split(",") if k.strip()]
    filtered_data_finale = filtered_data_finale.drop_nulls(subset=['Description'])
    filtered_data_finale = filtered_data_finale.with_columns(
        pl.col("Description").cast(pl.Utf8).str.to_lowercase()
    )
    for keyword in keywords_list:
        filtered_data_finale = filtered_data_finale.filter(
            pl.col("Description").str.contains(keyword, literal=True)
        )

col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.write(filtered_data_finale)
