# Import the dependencies.
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import pandas as pd

#################################################
# Database Setup
#################################################


# Create engine to connect to the SQLite database
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Reflect an existing database into a new model
base = automap_base()
base.prepare(engine, reflect=True)

# Save references to each table
measurement = base.classes.measurement
station = base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)


#################################################
# Flask Setup
#################################################

app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route('/')
def home():
    return (
        "Available Routes:<br/>"
        "/api/v1/stations<br/>"
        "/api/v1/precipitation<br/>"
        "/api/v1/tobs?station=<station_id><br/>"
        "/api/v1.0/<start><br/>"
        "/api/v1.0/<start>/<end>?"
    )

@app.route('/api/v1/precipitation')
def precipitation():
    most_recent = session.query(func.max(measurement.date)).scalar()
    most_recent_date = datetime.strptime(most_recent, '%Y-%m-%d')
    one_year_ago = most_recent_date - timedelta(days=365)
    
    results_precip = session.query(measurement.date, measurement.prcp).filter(
        measurement.date >= one_year_ago.strftime('%Y-%m-%d'),
        measurement.date <= most_recent_date.strftime('%Y-%m-%d')
    ).all()

@app.route('/api/v1/stations')
def stations():
    results_stations = session.query(station.station, station.name).all()
    stations_list = [{"station": station, "name": name} for station, name in results_stations]
    return jsonify(stations_list)

@app.route('/api/v1/tobs')
def tobs():
    station_id = request.args.get('station')
    if not station_id:
        return jsonify({"error": "Please provide a station ID"}), 400

    most_recent = session.query(func.max(measurement.date)).scalar()
    most_recent_date = datetime.strptime(most_recent, '%Y-%m-%d')
    one_year_ago = most_recent_date - timedelta(days=365)
    
    results = session.query(measurement.date, measurement.tobs).filter(
        measurement.station == station_id,
        measurement.date >= one_year_ago.strftime('%Y-%m-%d'),
        measurement.date <= most_recent_date.strftime('%Y-%m-%d')
    ).all()

    tobs_df = pd.DataFrame(results, columns=['Date', 'Temperature'])
    tobs_dict = tobs_df.set_index('Date').to_dict()['Temperature']
    return jsonify(tobs_dict)

@app.route('/api/v1.0/<start>', methods=['GET'])
@app.route('/api/v1.0/<start>/<end>', methods=['GET'])
def temperature_stats(start, end=None):
    # Check if 'start' is a valid date
    try:
        start_date = datetime.strptime(start, '%Y-%m-%d')
        if end:
            end_date = datetime.strptime(end, '%Y-%m-%d')
        else:
            end_date = None
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use YYYY-MM-DD."}), 400

    # Query temperature statistics
    query = session.query(
        func.min(measurement.tobs).label('TMIN'),
        func.avg(measurement.tobs).label('TAVG'),
        func.max(measurement.tobs).label('TMAX')
    ).filter(measurement.date >= start_date)

    if end_date:
        if end_date < start_date:
            return jsonify({"error": "End date must be after start date."}), 400
        query = query.filter(measurement.date <= end_date)

    # Execute query and fetch results
    results = query.one_or_none()

    # Return JSON response
    if results:
        return jsonify({
            "Start Date": start_date.strftime('%Y-%m-%d'),
            "End Date": end_date.strftime('%Y-%m-%d') if end_date else 'N/A',
            "TMIN": results.TMIN,
            "TAVG": results.TAVG,
            "TMAX": results.TMAX
        })
    else:
        return jsonify({"error": "No data found for the specified date range."}), 404

if __name__ == '__main__':
    app.run(debug=True)