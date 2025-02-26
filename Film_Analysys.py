#importiamo le librerie 
import polars as pl  
import streamlit as st
import altair as alt

Movies =  pl.read_csv("16k_Movies.csv") #importo il file csv
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
    pl.Series("minute", minuti, strict=False)
)
print(Movies)



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