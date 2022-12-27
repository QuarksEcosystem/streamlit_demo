from sqlite3 import Timestamp
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import pydeck as pdk
import snowflake.connector
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.pyplot import figure
import matplotlib.style as style
import geopandas
import altair as alt
import datetime
from PIL import Image

#Plot Style
style.use('fivethirtyeight')
plt.rcParams['lines.linewidth'] = 1
dpi = 1000
plt.rcParams['font.size'] = 13
#plt.rcParams['font.family'] = 'Times New Roman'
plt.rcParams['axes.labelsize'] = plt.rcParams['font.size']
plt.rcParams['axes.titlesize'] = plt.rcParams['font.size']
plt.rcParams['legend.fontsize'] = plt.rcParams['font.size']
plt.rcParams['xtick.labelsize'] = plt.rcParams['font.size']
plt.rcParams['ytick.labelsize'] = plt.rcParams['font.size']
plt.rcParams['figure.figsize'] = 8, 8

#Page layout config and title
st.set_page_config(layout="wide", page_title="Traffic and Weather Events Demo", page_icon=":car:")
st.title('Eventos de Trânsito e Clima nos EUA')

#Add image
coli, colt = st.columns([10, 13])
image = Image.open('images/Weather.jpeg')
with coli:  
    st.image(image,caption='Source: https://tempuslogix.com/how-do-weather-events-affect-roads-1/ ')
with colt:
    st.text("A partir de dados históricos de trânsito e clima no EUA foram criados modelos de transformação de dados\n\
utilizando o DBT e modelos de aprendizagem de máquina utilizando o Dataiku. O resultado desses modelos\n\
podem ser conferidos interativamente nos gráficos gerados por meio do Streamlit.")


# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

conn = init_connection()

# Perform query.
# Uses st.experimental_memo to only rerun when the query changes
@st.experimental_memo
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

# Perform query and returns dataframe
# Uses st.experimental_memo to only rerun when the query changes
@st.experimental_memo
def query_dataframe(query):
    with conn.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
        if data:
            df = pd.DataFrame(data)
        return df

# creating date selection on small columns
col1, col2, col3 = st.columns([1, 1, 3])
with col1:
    d = st.date_input(
        "Starting Date",
        datetime.date(2016, 8, 1))
        
with col2:
    d2 = st.date_input(
        "Ending Date",
        datetime.date(2019, 7, 6))

# Garantindo que as datas estão no intervalo de tempo dos datasets
if(d < datetime.date(2016, 8, 1)):
    d = datetime.date(2016, 8, 1)

if(d2 > datetime.date(2020, 12, 31)):
    d2 = datetime.date(2020, 12, 31)

if(d > d2):
    d_temp = d
    d = d2
    d2 = d_temp

start = d.strftime('%m/%d/%Y')
end = d2.strftime('%m/%d/%Y')

run_query("use warehouse DEV_DEMO_WH;")
st.header('Análise dos eventos de Clima e Trânsito no período de ' + d.strftime('%Y') + ' a ' + d2.strftime('%Y'))

#Colunas para o plot dos dois primeiros gráficos
col1, col2 = st.columns(2)

#plot do primeiro gráfico
with col1:
    st.text('(a) No gráfico abaixo é possível comparar a média de eventos de trânsito que ocorrem em uma cidade do EUA quando\nestá ocorrendo, ou não, algum evento climático.')
    bigcities_events_clean_weather = run_query("SELECT TO_TIME(event_timestamp) as hour, count(traffic_events), count(distinct date_trunc(day,  event_timestamp), city)  from TRAFFIC_WEATHER.TRAFFIC_WEATHER_PROD.BIGCITIES_TIMED_EVENTS where weather_event is null and event_timestamp > '" + start + "' and event_timestamp < '" + end + "' group by hour order by hour ;")

    hour_clean = [row[0] for row in bigcities_events_clean_weather]

    traffic_event_clean = [row[1] for row in bigcities_events_clean_weather]

    distinc_date_city_clean = [row[2] for row in bigcities_events_clean_weather]

    average_clean = [i / j for i, j in zip(traffic_event_clean, distinc_date_city_clean)]

    fig, ax = plt.subplots(figsize=(8, 6), dpi=160)
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%H:%M'))
    ax.xaxis.set_label_text("Horário do Dia")
    ax.yaxis.set_label_text("Média de eventos de trânsito")

    ax.set_title('Média de eventos de trânsito durante o dia.', fontstyle='italic')
    my_day = datetime.date(2020, 1, 1)
    x_dt = [ datetime.datetime.combine(my_day, t) for t in hour_clean ]

    #limite do eixo y de 0 a 4
    plt.ylim([0, 4])

    ax.plot(x_dt, average_clean, label = "Clima bom")

    bigcities_events_bad_weather = run_query("SELECT TO_TIME(event_timestamp) as hour, count(traffic_events), count(distinct date_trunc(day,  event_timestamp), city)  from TRAFFIC_WEATHER.TRAFFIC_WEATHER_PROD.BIGCITIES_TIMED_EVENTS where weather_event is not null and event_timestamp > '" + start + "' and event_timestamp < '" + end + "' group by hour order by hour ;")

    hour_bad_weather = [row[0] for row in bigcities_events_bad_weather]

    traffic_event_bad_weather = [row[1] for row in bigcities_events_bad_weather]

    distinc_date_city_bad_weather = [row[2] for row in bigcities_events_bad_weather]

    average_bad_weather = [i / j for i, j in zip(traffic_event_bad_weather, distinc_date_city_bad_weather)]

    my_day = datetime.date(2020, 1, 1)
    x_dt = [ datetime.datetime.combine(my_day, t) for t in hour_bad_weather ]
    plt.ylim([0, 4])
    ax.plot(x_dt, average_bad_weather, label = "Clima ruim")

    ax.legend()

    st.pyplot(fig)



#plot do segundo gráfico
with col2:
    # Average Traffic Events by Weather Events Number
    st.text('(b) Neste gráfico é possível observar como a quantidade de eventos climáticos afetam a quantidade de eventos de\ntrânsito que ocorrem no EUA.')
    average_traffic_by_weather = run_query("select weather_events, avg(traffic_events) from TRAFFIC_WEATHER.TRAFFIC_WEATHER_PROD.DAILY_EVENTS_BY_CITY group by weather_events order by weather_events;")

    avg_traffic = [row[1] for row in average_traffic_by_weather]

    weather_events = [row[0] for row in average_traffic_by_weather]

    range = ['0-19','20-39', '40-59', '60-79']

    total_in_range = [0, 0, 0, 0]

    total_entries = [0, 0, 0, 0]

    for idx, x in enumerate(weather_events):
        if x <20:
            total_entries[0] += 1
            total_in_range[0] += avg_traffic[idx]
        elif x < 40:
            total_entries[1] += 1
            total_in_range[1] += avg_traffic[idx]
        elif x < 60:
            total_entries[2] += 1
            total_in_range[2] += avg_traffic[idx]
        elif x < 80:
            total_entries[3] += 1
            total_in_range[3] += avg_traffic[idx]

    average_in_ranges = [i / j for i, j in zip(total_in_range, total_entries)]

    fig3, ax3 = plt.subplots(figsize=(8, 6), dpi=160)

    ax3.set_title('Média diária de eventos de trânsito por quantidade de eventos climáticos', fontstyle='italic')

    ax3.xaxis.set_label_text("Eventos Climáticos")
    ax3.yaxis.set_label_text("Eventos de Trânsito")

    #plot em forma de barras
    ax3.bar(range, average_in_ranges)

    st.pyplot(fig3)

# Frequency Distribution of Traffic and Weather Events from Aug 2016 to Dec 2020
#colunas para o plot dos gráficos de distribuição de eventos nos estados
st.text('(c) Os dois gráficos abaixo representam a distribuição de frequência dos eventos por estado do EUA.')
col1, col2= st.columns(2)

df = query_dataframe("select * from TRAFFIC_WEATHER.TRAFFIC_WEATHER_PROD.TOTAL_EVENTS_BY_STATE;")
#Renomeia as colunas do dataframe
if not df.empty:
    df = df.rename(columns={df.columns[0]: "STUSPS", df.columns[1]: "TotalTraffic", df.columns[2]: "TotalWeather"})
    #st.write(df)

# Pega as coordenadas e dimensões dos estados do EUA para plot no gráfico
states = geopandas.read_file('data/usa-states-census-2014.shp')

#merge de dados do dataframe com os dados do geopandas
map_and_stats=states.merge(df, on="STUSPS")

#st.write(map_and_stats)

#Plota distribuição dos eventos de trânsito nos estados

with col1:
    
    fig4, ax4 = plt.subplots(1, figsize=(10, 10))
    #rotaciona a legenda dos ticks do eixo x em 90 graus
    plt.xticks(rotation=90)

    ax4.set_title('Distribuição de Frequência de Eventos de Trânsito de 2016 a 2020', fontstyle='italic')

    map_and_stats.plot(column="TotalTraffic", cmap="Reds", linewidth=0.4, ax=ax4, edgecolor=".4")

    traffic_column = map_and_stats["TotalTraffic"]
    max_traffic = traffic_column.max()

    bar_info = plt.cm.ScalarMappable(cmap="Reds", norm=plt.Normalize(vmin=0, vmax=max_traffic))
    bar_info._A = []
    cbar = fig4.colorbar(bar_info,fraction=0.02, pad=0.07)

    ax4.xaxis.set_label_text("Longitude")
    ax4.yaxis.set_label_text("Latitude")

    st.pyplot(fig4)

#Plota distribuição dos eventos climáticos nos estados
with col2:

    fig5, ax5 = plt.subplots(1, figsize=(10, 10))
    plt.xticks(rotation=90)

    ax5.set_title('Distribuição de Frequência de Eventos Climáticos de 2016 a 2020', fontstyle='italic')

    map_and_stats.plot(column="TotalWeather", cmap="Blues", linewidth=0.4, ax=ax5, edgecolor=".4")

    weather_column = map_and_stats["TotalWeather"]
    max_weather = weather_column.max()

    bar_info = plt.cm.ScalarMappable(cmap="Blues", norm=plt.Normalize(vmin=0, vmax=max_weather))
    bar_info._A = []
    cbar = fig5.colorbar(bar_info,fraction=0.02, pad=0.000)

    ax5.xaxis.set_label_text("Longitude")
    ax5.yaxis.set_label_text("Latitude")

    st.pyplot(fig5)

#Show dataiku model results
st.header("Resultados de Aprendizado de Máquina com Dataiku")

# with st.expander("Machine Learning Model Analysis"):

#     image = Image.open('images/TrafficEvents_by_WeatherEvents.png')

#     st.image(image)

#     image2 = Image.open('images/TrafficEvents_by_WeatherSeverity.png')

#     st.image(image2)

#     image3 = Image.open('images/TrafficEvents_by_Hour.png')

#     st.image(image3)

#     image4 = Image.open('images/AvgofprobababilitiesofTraffic_EventsbyHourofDayonClearWeather.png')

#     st.image(image4)

#     image5 = Image.open('images/AvgofTrafficEventsbyHoursonBadWeather.png')

#     st.image(image5)

#     image6 = Image.open('images/AvgofTraffic_SeveritybyWeather_Severity.png')

#     st.image(image6)

#     image7 = Image.open('images/AvgofTraffic_SeveritybyHour.png')

#     st.image(image7)


#query for dataiku model prediction results
total_events_per_month = run_query('SELECT DATE_TRUNC(Month,"EVENT_TIMESTAMP") as "Months", count(case when (TRAFFIC_EVENT != \'None\' and (TO_TIME("EVENT_TIMESTAMP") >= \'04:15:00\' and TO_TIME("EVENT_TIMESTAMP") <= \'20:45:00\')) then 1 end) as real_event, count(case when ("prediction" != \'None\' and (TO_TIME("EVENT_TIMESTAMP") >= \'04:15:00\' and TO_TIME("EVENT_TIMESTAMP") <= \'20:45:00\')) then 1 end) as predicted from DATAIKU_DATABASE.DATAIKU_SCHEMA.BIGCITIES_TIMED_EVENTS_PREDICTION_WEATHERTRAFFICEVENTS group by "Months" order by "Months";')

days = [row[0] for row in total_events_per_month]

traffic = [row[1] for row in total_events_per_month]

prediction = [row[2] for row in total_events_per_month]

with st.expander("Predição de eventos trânsico com dados climáticos de 2019"):
    st.text("Utilizando a ferramenta de aprendizado de máquina Dataiku foi criado um modelo de predição de eventos de trânsito e se realizou um teste\n\
onde os dados de clima de 2019 foram utilizados para a predição dos eventos de trânsito e o resultado foi comparada aos dados reais de 2019 para a validação do modelo.\n\
O resultado dessa validação pode ser observada nos gráficos abaixo.\n\
A precisão atingida pelo modelo é de 0.772 com um tempo de treinamento de apenas 37 minutos.")
    col1, col2, col3= st.columns(3)
    with col1:
        fig, ax = plt.subplots(figsize=(8, 6), dpi=160)
        plt.ticklabel_format(useMathText=True)
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_label_text("Meses")
        ax.yaxis.set_label_text("Eventos")

        ax.set_title('Total mensal de eventos de trânsito nas 200 maiores cidade do EUA em 2019 (Real)', fontstyle='italic')

        ax.bar(days, traffic, width = 20)

        st.pyplot(fig)

    with col2:
        fig, ax = plt.subplots(figsize=(8, 6), dpi=160)
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
        ax.xaxis.set_label_text("Meses")
        ax.yaxis.set_label_text("Eventos")

        ax.set_title('Total mensal de eventos de trânsitos nas 200 maiores cidade do EUA em 2019 (Predição)', fontstyle='italic')

        ax.bar(days, prediction, width = 20)

        st.pyplot(fig)

    with col3:
        fig, ax = plt.subplots(figsize=(8, 6), dpi=160)
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
        ax.xaxis.set_label_text("Meses")
        ax.yaxis.set_label_text("Eventos")

        ax.set_title('Total mensal de eventos de trânsito nas 200 maiores cidade do EUA em 2019 (Comparação)', fontstyle='italic')

        ax.bar(days, prediction, width = 20, label="Previsão")
        ax.bar(days, traffic, width = 20, label="Real")
        ax.legend()

        st.pyplot(fig)

    # Date input para a visualização de um dia específico
    col1, col2, col3 = st.columns([3, 1, 1])
    with col2:
        d = st.date_input(
                "Selecione um dia de semana em 2019 para visualizar os valores reais e previstos da quantidade de eventos durante um dia.",
                datetime.date(2019, 1, 24))

        if(d < datetime.date(2019, 1, 1)):
            d = datetime.date(2019, 1, 1)

        if(d > datetime.date(2019, 12, 31)):
            d = datetime.date(2019, 12, 31)

    
    date = d.strftime('%Y-%m-%d')+'T00:00:00Z'

    run_query('use warehouse dev_demo_wh;')
    day_cities_event = run_query('SELECT DATE_TRUNC(day,"EVENT_TIMESTAMP") as days, TO_TIME("EVENT_TIMESTAMP") as hour,  count(case when TRAFFIC_EVENT != \'None\' then 1 end) as real_event, count(case when "prediction" != \'None\' then 1 end) as predicted from DATAIKU_DATABASE.DATAIKU_SCHEMA.BIGCITIES_TIMED_EVENTS_PREDICTION_WEATHERTRAFFICEVENTS where (days =\'' + date + '\' and hour >= \'04:15:00\' and hour <= \'20:45:00\' ) group by days, hour order by hour;')

    hours = [row[1] for row in day_cities_event]
    traffic_events = [row[2] for row in day_cities_event]
    prediction = [row[3] for row in day_cities_event]

    fig, ax = plt.subplots(figsize=(8, 6), dpi=160)
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%H:%M'))
    ax.xaxis.set_label_text("Horário do Dia")
    ax.yaxis.set_label_text("Número de Eventos de Trânsito")

    ax.set_title('Número de eventos de trânsito reais e previstos no dia '+ d.strftime('%Y-%m-%d'), fontstyle='italic')
    my_day = datetime.date(2019, 1, 1)
    x_dt = [ datetime.datetime.combine(my_day, t) for t in hours ]

    ax.plot(x_dt, prediction, label = "Eventos Previstos")
    ax.plot(x_dt, traffic_events, label = "Eventos Reais")
    ax.legend()
    plt.ylim([0, 600])

    with col1:
        st.pyplot(fig)

with st.expander("Previsão de Eventos de Trânsito em ruas de Houston"):
    st.text('Por meio de dados reais obtidos de uma API de previsão do clima e dados históricos das 100 ruas de Houston com maior quantidade de \n\
eventos de trânsito, foram geradas previsões de eventos climáticos para cada uma dessas ruas estudadas.')
    streets = query_dataframe('select distinct street from DATAIKU_DATABASE.DATAIKU_SCHEMA.HOUSTON_ML_PREDICTED_WEATHERTRAFFICEVENTS;')
    street = st.selectbox('Ruas', streets, index=0)
    dates = query_dataframe('select distinct to_date(event_timestamp) as date from DATAIKU_DATABASE.DATAIKU_SCHEMA.HOUSTON_ML_PREDICTED_WEATHERTRAFFICEVENTS order by date;')
    d = st.selectbox('Dia', dates, index=0)
    date = d.strftime('%Y-%m-%d')
    ax.set_title('Teste')
    df = query_dataframe('select street, "prediction" as predicted_traffic_event, weather_event, TO_TIMESTAMP_NTZ(event_timestamp) from DATAIKU_DATABASE.DATAIKU_SCHEMA.HOUSTON_ML_PREDICTED_WEATHERTRAFFICEVENTS where to_date(EVENT_TIMESTAMP) = \'' + date + '\' and street = \'' + street + '\' order by event_timestamp;')
    df = df.rename(columns={df.columns[0]: "Rua", df.columns[1]: "Previsão do Trânsito", df.columns[2]: "Previsão do Tempo", df.columns[2]: "Hora"})
    st.write(df)
    