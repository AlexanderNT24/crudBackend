from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS  # Importar CORS

app = Flask(__name__)
CORS(app)  # Habilitar CORS para todas las rutas

app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///personas.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

class Persona(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(100), nullable=False)
    edad = db.Column(db.Integer, nullable=False)
    genero = db.Column(db.String(50), nullable=False)
    activo = db.Column(db.Boolean, default=True)

# Crear la base de datos
with app.app_context():
    db.create_all()

# Obtener todas las personas
@app.route("/personas", methods=["GET"])
def get_personas():
    personas = Persona.query.all()
    return jsonify([
        {"id": p.id, "nombre": p.nombre, "edad": p.edad, "genero": p.genero, "activo": p.activo}
        for p in personas
    ])

# Agregar una nueva persona
@app.route("/personas", methods=["POST"])
def add_persona():
    data = request.json
    print(data)
    nueva_persona = Persona(
        nombre=data["nombre"], 
        edad=data["edad"], 
        genero=data["genero"],
        activo=data.get("activo", True)
    )
    db.session.add(nueva_persona)
    db.session.commit()
    
    return jsonify({"mensaje": "Persona agregada", "persona": {
        "id": nueva_persona.id, 
        "nombre": nueva_persona.nombre, 
        "edad": nueva_persona.edad, 
        "genero": nueva_persona.genero,
        "activo": nueva_persona.activo
    }})

# Ejecutar el servidor
if __name__ == "__main__":
    app.run(debug=True)
