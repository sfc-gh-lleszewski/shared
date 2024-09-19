# Zaimportuj niezbędne pakiety
import streamlit as st

from snowflake.snowpark.context import get_active_session
import pandas as pd

# Pobranie aktywnej sesji Snowflake
session = get_active_session()
# Pobierz dane z lobster_sales oraz lobser_sales_forecast
df_real = session.table("QUICKSTART.ML_FUNCTIONS.LOBSTER_SALES").to_pandas()
df_forecast = session.table("QUICKSTART.ML_FUNCTIONS.lobster_sales_forecast").to_pandas()

#Posortuj dane po TIMESTAMP
df_real = df_real.sort_values('TIMESTAMP')
df_forecast = df_forecast.sort_values('TIMESTAMP')


# Tytuł
st.title("Vancouver sales analyze")

tab1, tab2, tab3 = st.tabs(["Compare", "Forecast", "Anomaly"])

with tab1:
    # Wyświetlenie nagłówka
    st.header(":lobster: Lobster sales analyze")
    # Podzielenie strony na 2 kolumny
    col1, col2 = st.columns(2);
    # Wyświetlenie danych w pierwszej kolumnie
    with col1:
        st.subheader("Real sales (last year)");
        st.dataframe(df_real,use_container_width=True);
        # Wyświetlenie danych w drugiej kolumnie
    with col2:
        st.subheader("Forecast (last year)");
        st.dataframe(df_forecast,use_container_width=True);
    # Wyświetl nagłówek drugiego stopnia
    st.subheader("Compare real vs forecast");
    
    # Połącz wyniki danych realnych z predykcją
    df_chart=pd.merge(df_real, df_forecast, on='TIMESTAMP')
    # Wyświetl połączony wynik
    # Wyświetl wykres
    compare_chart=st.line_chart(df_chart,x="TIMESTAMP",y=["TOTAL_SOLD","FORECAST"],color=["#FF0000","#0000FF"]
    )

with tab2:
    # dodatkowa treść
    st.write(
    """Application displays real sales of "Lobster" menu item vs forecast data. Click
    **"Prepare forecast"** to calculate forecast for next days. In the last section we
    can find anomalies for Vancouver sales
    """
    )
    
    # Wyświetlenie nagłówka
    st.header(":lobster: Lobster sales analyze")

    # Dodaj nagłówek drugiego stopnia
    st.subheader("Forecast for next days");
    # Pobierz od użytkownika liczbę dni
    days_ahead = st.number_input("Provide number of days",min_value=1, max_value=1000, value=10, step=1)
    
    # Dodanie przycisku do obliczenia kalkulacji
    if st.button("Forecast calculate"):
        # Uruchom przeliczenie prognozy
        df_res = session.call("QUICKSTART.ML_FUNCTIONS.CALCULATE_FORECAST",days_ahead).to_pandas();
        # Przesortuj dane wynikowe
        df_res = df_res.sort_values('TIMESTAMP')
        # Wyświetl wynik
        st.dataframe(df_res,use_container_width=True);
        # Dołącz dane prognozowane do rzeczywistych
        df_chart_total=pd.concat([df_real,df_res], ignore_index=True)
        # Wyświetl wykres
        total_chart=st.line_chart(df_chart_total,x="TIMESTAMP",y=["FORECAST","TOTAL_SOLD"],color=["#FFFF00","#0000FF"])

with tab3:
    # Dodaj nagłówek Anomalies
    st.header("Anomalies")
    # Pobierz dane z vancouver_anomalies
    df_anomalies = session.table("QUICKSTART.ML_FUNCTIONS.VANCOUVER_ANOMALIES").to_pandas();
    menu_item = st.selectbox("Menu item:", df_anomalies["SERIES"].unique())
    df_selected = df_anomalies.loc[df_anomalies["SERIES"]==menu_item]
    
    # Zdefiniuj podświetlenie
    def highight_anomalies(s):
        return ['background-color: red']*len(s) if s.IS_ANOMALY else ['background-color: transparent']*len(s)
        
    # Posortuj
    df_anomalies = df_anomalies.sort_values('TS')
    # Wyświetl wynik
    st.dataframe(df_selected.style.apply(highight_anomalies,axis=1),use_container_width=True);