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
colorscale = ['red', 'yellow', 'green']#, 'blue', 'purple']  # rainbow
chroma = "https://cdnjs.cloudflare.com/ajax/libs/chroma-js/2.1.0/chroma.min.js"  # js lib used for colors
color_prop = 'rank'

# js function for adding points to layers
point_to_layer = assign("""function(feature, latlng, context){
        const {min, max, colorscale, circleOptions, colorProp} = context.props.hideout;
        const csc = chroma.scale(colorscale).domain([min, max]);  // chroma lib to construct colorscale
        circleOptions.fillColor = csc(feature.properties[colorProp]);  // set color based on color prop.
        return L.circleMarker(latlng, circleOptions);  // sender a simple circle marker.
    }""")

cluster_to_layer = assign("""function(feature, latlng, index, context){
    const {min, max, colorscale, circleOptions, colorProp} = context.props.hideout;
    const csc = chroma.scale(colorscale).domain([min, max]);
    // Set color based on mean value of leaves.
    const leaves = index.getLeaves(feature.properties.cluster_id);
    let valueSum = 0;
    for (let i = 0; i < leaves.length; ++i) {
        valueSum += leaves[i].properties[colorProp]
    }
    const valueMean = valueSum / leaves.length;
    // Render a circle with the number of leaves written in the center.
    const icon = L.divIcon.scatter({
        html: '<div style="background-color:white;"><span>' + feature.properties.point_count_abbreviated + '</span></div>',
        className: "marker-cluster",
        iconSize: L.point(40, 40),
        color: csc(valueMean)
    });
    return L.marker(latlng, {icon : icon})
}""")

# instantiate app
app = Dash(__name__,
        external_stylesheets=external_stylesheets,
        external_scripts=[chroma]
    )

server = app.server

# Create app layout
app.layout = html.Div([
    dbc.Row([
        dbc.Col(html.Div(html.Img(src='/assets/sage_logo.png',style={'maxWidth': '50%'}), style={'textAlign': 'center'})),
        dbc.Col(html.H1('One Tree Planted - Sage Hackathon', style={'textAlign': 'center'})),
        dbc.Col(html.Div(html.Img(src='/assets/one_tree_planted_logo.png',style={'maxWidth': '50%'}), style={'textAlign': 'center'})),
        ], justify="center", align="center", className="h-50"),
    dbc.Row([
        dbc.Col(html.H3('Select level of aggregation'), width={'size':2, 'offset':0}),
        dbc.Col(html.H3('Select number to show'), width={'size':2, 'offset':0})
        ], justify="center", align="center", className="h-50"),
    dbc.Row([
         dbc.Col(dcc.RadioItems(
             [
                 {'label':'Census Tract', 'value':'census_tract'},
                 {'label':'County', 'value':'county'},
                 {'label':'City', 'value': 'city'},
                 {'label':'State', 'value':'state'}
             ],
                'city',
                id='data-type',
                inline=True
                ), width={'size':2, 'offset':0}),
         dbc.Col(dcc.Dropdown(
             [20,50,100,200,500], 20,
             id='n-dropdown'
             ),width={'size':2, 'offset':0}),
         ],justify="center", align="center", className="h-50"),
    html.Div(id='map-data', style={'marginTop':'10px'}),
    html.Div(id='data-table', style={'marginTop': '20px'})
    ])
    

@app.callback([
    Output('map-data', 'children'),
    Output('data-table', 'children')
    ],
    [Input('data-type', 'value'),
     Input('n-dropdown', 'value')
    ]
)
def update_map(data_type, n_values):

    tooltip_prop = {
        'census_tract': 'GEOID10',
        'county': 'CF',
        'state': 'SF',
        'city': 'city'
    }

    #if data_type != 'city':
    #    df = pd.read_csv(f'{data_type}_centres.csv', index_col=0)  
    #else:
    df = pd.read_csv(f'{data_type}_centres.csv')  

    df = df.sort_values('rank').iloc[:n_values].dropna()

    dicts = df.to_dict('records')
    for item in dicts:
        item['tooltip'] = '{} ({:.1f})'.format(item[data_type], item[color_prop]) # bind tooltip

    # Data for table
    data_table = dbc.Row([
        dbc.Col(html.P('Example text goes here'), width={'size':4, 'offset':1}),
        dbc.Col(
            dash_table.DataTable(df.to_dict('records'), 
            [{"name": i.title(), "id": i} for i in df.columns if not i in ('lat', 'lon')],
            editable=True,
            filter_action="native",
            sort_action="native",
            sort_mode='multi',
            row_selectable='multi',
            page_action='native',
            page_current= 0,
            page_size= 10,
            ), 
        width={'size':4, 'offset':1})
    ])

    geojson = dlx.dicts_to_geojson(dicts)
    geobuf = dlx.geojson_to_geobuf(geojson)  

    vmax = df[color_prop].max()
    colorbar = dl.Colorbar(colorscale=colorscale, width=20, height=150, min=0, max=vmax, unit='Rank')

    # Geojson rendering logic, must be JavaScript as it is executed in clientside.

    geojson = dl.GeoJSON(data=geobuf, id="geojson", format="geobuf",
                     zoomToBounds=True,  # when true, zooms to bounds when data changes
                     #cluster=True,
                     zoomToBoundsOnClick=True,
                     #clusterToLayer=cluster_to_layer,  # how to draw points
                     options=dict(pointToLayer=point_to_layer),
                     #superClusterOptions=dict(radius=150),   # adjust cluster size
                     hideout=dict(colorProp=color_prop, circleOptions=dict(fillOpacity=0.6, stroke=False, radius=20),
                                  min=0, max=vmax, colorscale=colorscale))


    return dl.Map([dl.TileLayer(), geojson, colorbar], center=(40.32, -101.18), zoom=3, style={'width': '80%', 'height': '50vh', 'margin': "auto", "display": "block"}), data_table


if __name__ == '__main__':
    app.run_server(debug=True)

