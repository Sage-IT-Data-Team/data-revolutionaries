import json
import dash_leaflet as dl
import dash_leaflet.express as dlx
from dash import Dash, html, dcc, Input, Output, dash_table
import dash_bootstrap_components as dbc
from dash_extensions.javascript import assign
import pandas as pd


# Set styling
external_stylesheets = [
    'https://codepen.io/chriddyp/pen/bWLwgP.css',
    {
        'href': 'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css',
        'rel': 'stylesheet',
        'integrity': 'sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO',
        'crossorigin': 'anonymous'
    }
]

# Set color properties
colorscale = ['red', 'yellow', 'green', 'blue', 'purple']  # rainbow
chroma = "https://cdnjs.cloudflare.com/ajax/libs/chroma-js/2.1.0/chroma.min.js"  # js lib used for colors
color_prop = 'geo_size'

# js function for adding points to layers
point_to_layer = assign("""function(feature, latlng, context){
        const {min, max, colorscale, circleOptions, colorProp} = context.props.hideout;
        const csc = chroma.scale(colorscale).domain([min, max]);  // chroma lib to construct colorscale
        circleOptions.fillColor = csc(feature.properties[colorProp]);  // set color based on color prop.
        return L.circleMarker(latlng, circleOptions);  // sender a simple circle marker.
    }""")


# instantiate app
app = Dash(__name__,
        external_stylesheets=external_stylesheets,
        external_scripts=[chroma]
    )

# Create app layout
app.layout = html.Div([
    dbc.Row([
        dbc.Col(html.Div(html.Img(src='/assets/sage_logo.png',style={'maxWidth': '50%'}), style={'textAlign': 'center'})),
        dbc.Col(html.H1('One Tree Planted - Sage Hackathon', style={'textAlign': 'center'})),
        dbc.Col(html.Div(html.Img(src='/assets/one_tree_planted_logo.png',style={'maxWidth': '50%'}), style={'textAlign': 'center'})),
        ], justify="center", align="center", className="h-50"),
    dbc.Row([
         dcc.RadioItems(
                ['Census Tract', 'County', 'State'],
                'County',
                id='data-type',
                inline=True
            )
         ],justify="center", align="center", className="h-50"),
    html.Div(id='map-data'),
    html.Div(id='data-table', style={'marginTop': '20px'})
    ])
    

@app.callback([
    Output('map-data', 'children'),
    Output('data-table', 'children')
    ],
    Input('data-type', 'value')
)
def update_map(data_type):

    tooltip_prop = {
        'census_tract': 'GEOID10',
        'county': 'CF',
        'state': 'SF'
    }

    data_type = data_type.lower().split(' ')
    data_type = '_'.join(data_type)

    df = pd.read_csv(f'{data_type}_centres.csv', index_col=0)  
    df['geo_size'] *= 10

    dicts = df.to_dict('records')
    for item in dicts:
        item['tooltip'] = '{} ({:.1f})'.format(item[tooltip_prop[data_type]], item[color_prop]) # bind tooltip

    # Data for table
    data_table = dbc.Row([
        dbc.Col(html.P('Example text goes here'), width={'size':4, 'offset':1}),
        dbc.Col(
            dash_table.DataTable(df.to_dict('records'), 
            [{"name": i, "id": i} for i in df.columns],
            page_action="native",
            page_current= 0,
            page_size= 10,
            ), 
        width={'size':4, 'offset':1})
    ])

    geojson = dlx.dicts_to_geojson(dicts)
    geobuf = dlx.geojson_to_geobuf(geojson)  

    vmax = df[color_prop].max()
    colorbar = dl.Colorbar(colorscale=colorscale, width=20, height=150, min=0, max=vmax, unit='area')

    # Geojson rendering logic, must be JavaScript as it is executed in clientside.

    geojson = dl.GeoJSON(data=geobuf, id="geojson", format="geobuf",
                     zoomToBounds=True,  # when true, zooms to bounds when data changes
                     options=dict(pointToLayer=point_to_layer),  # how to draw points
                     superClusterOptions=dict(radius=50),   # adjust cluster size
                     hideout=dict(colorProp=color_prop, circleOptions=dict(fillOpacity=1, stroke=False, radius=5),
                                  min=0, max=vmax, colorscale=colorscale))


    return dl.Map([dl.TileLayer(), geojson, colorbar], center=(40.32, -101.18), zoom=3, style={'width': '80%', 'height': '50vh', 'margin': "auto", "display": "block"}), data_table


if __name__ == '__main__':
    app.run_server(debug=True)

