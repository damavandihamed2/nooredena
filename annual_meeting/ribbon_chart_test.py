import itertools
import numpy as np
import pandas as pd

import plotly.express as px
import plotly.graph_objects as go


def get_val(x, sub=0.1, axis='x'):
    if axis != 'x':
        a = [[i, i]for i in x]
    else:
        a = [[i-sub, i+sub]for i in x]
    return list(itertools.chain.from_iterable(a))


def getupperlower(brand):
    ch1 = final.query('brands == @brand')
    upper_col = [i for i in ch1.columns if 'upper' in i]
    lower_col = [i for i in ch1.columns if 'lower' in i]
    upper_data = ch1[upper_col].values.tolist()[0]
    lower_data = ch1[lower_col].values.tolist()[0]
    annotate_place = ch1['y0'].values.tolist()[0]
    return upper_data, lower_data, annotate_place


text = """brands q2_2021 q3_2021 q4_2021 q1_2022 q2_2022 q3_2022
BYD 0.07 0.11 0.12 0.14 0.16 0.2
Tesla 0.15 0.15 0.14 0.16 0.12 0.13
Wuling 0.07 0.06 0.05 0.06 0.05 0.05
Volkswagen 0.07 0.06 0.05 0.04 0.04 0.04
GAC 0.02 0.02 0.02 0.02 0.03 0.03
Others 0.62 0.60 0.62 0.58 0.6 0.55"""
data = [i.split(' ') for i in text.split("\n")]
df = pd.DataFrame(data[1:], columns=data[0])
to_join = df.iloc[:, 1:].astype('float')*100
df = pd.concat([df['brands'], to_join], axis=1)

final = pd.DataFrame(df.brands)
for i, col in enumerate(['q2_2021', 'q3_2021', 'q4_2021', 'q1_2022', 'q2_2022', 'q3_2022']):
    ch1 = df.loc[:, ['brands', col]]
    ch1.sort_values(col, inplace=True)
    ch1[f'y{i}_upper'] = ch1[col].cumsum()
    ch1[f'y{i}_lower'] = ch1[f'y{i}_upper'].shift(1)
    ch1 = ch1.fillna(0)
    ch1[f'y{i}'] = ch1.apply(lambda x: (x[f'y{i}_upper']+x[f'y{i}_lower'])/2, axis=1)
    final = final.merge(ch1.iloc[:, [0, 2, 3, 4]], on='brands')

colors = {'BYD Auto': '#72c6e8', 'Tesla': '#E41A37', 'Wuling': '#5c606d', 'Volkswagen': '#12618F',
          'GAC Motor': '#d9871b', 'Others': 'rgba(0,0,0,0.1)'}
colors2 = [i for i in colors.values()]

fig = go.Figure()

x = (np.arange(df.shape[1] - 1) + 1).tolist()
x = get_val(x, sub=0.15)
x_rev = x[::-1]

annotations = []

for i, brand in enumerate(df.brands):
    upper_col, lower_col, annotate_place = getupperlower(brand)
    y_upper = get_val(upper_col, axis='y')
    y_lower = get_val(lower_col, axis='y')
    y_lower = y_lower[::-1]
    fig.add_trace(go.Scatter(
        x=x + [x[-1] + 1, x[-1] + 1] + x_rev,
        y=y_upper + [y_upper[-1], y_lower[0]] + y_lower,
        fill='toself',
        fillcolor=colors2[i],
        opacity=0.75,
        line_color='rgba(0,0,0,0.2)',
        showlegend=False,
        name=brand,
        line_shape='spline',
        mode='lines',
        hovertemplate=' '
    ))
    annotations.append(dict(xref='paper', yref='y',
                            x=-0.005, y=annotate_place,
                            text=brand, align="right", xanchor='right',
                            font=dict(family='Arial', size=14,
                                      color=colors2[i]),
                            showarrow=False))

ch1 = df.set_index('brands')
for i, j in enumerate(['q2_2021', 'q3_2021', 'q4_2021', 'q1_2022', 'q2_2022', 'q3_2022']):

    ch2 = pd.DataFrame(ch1.T.iloc[i])
    ch2.columns = [i + 1]
    ch2.sort_values(i + 1, ascending=True, inplace=True)
    fig_px = px.bar(ch2.T, color_discrete_map=colors, opacity=0.7, text_auto=True)
    fig_px.update_traces(hovertemplate='<b>%{x}</b>, %{value}%')
    fig_px.update_traces(textfont=dict(size=10, color='black'), textposition='auto', cliponaxis=False,
                         texttemplate='%{value}%')
    for trace in fig_px['data']:
        fig.add_trace(trace)

annotation = []

fig.update_layout(barmode='stack', bargap=0.9, showlegend=False)

fig.update_layout(plot_bgcolor='#f2f3f4', paper_bgcolor='#f2f3f4', margin=dict(l=100, b=10, r=100))
fig.update_xaxes(range=[0.85, 6.15], tickmode='array', showticklabels=True,
                 ticktext=['q2_2021', 'q3_2021', 'q4_2021', 'q1_2022', 'q2_2022', 'q3_2022'],
                 tickvals=[1, 2, 3, 4, 5, 6], fixedrange=True)
fig.update_yaxes(range=[0, 101], showticklabels=False, showgrid=False, fixedrange=True)
fig.update_layout(annotations=annotations)
fig.update_layout(title='Global Passenger Electric Vehicle Market Share, Q2 2021 - Q3 2022')

fig.write_html("C:/Users/h.damavandi/desktop/fig_4.html")
