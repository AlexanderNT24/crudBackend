from flask import Blueprint, jsonify
from app.services.data_service import load_data

fleet_bp = Blueprint('/data-rambo', __name__)

@fleet_bp.route('/', methods=['GET'])
def get_dynamic_data():
    data = load_data()
    return jsonify(data) 
