#importiamo le librerie 
import polars as pl  
import streamlit as st
import altair as alt

data =  pl.read_csv("Impact_of_Remote_Work_on_Mental_Health.csv") #importo il file csv
print(data)