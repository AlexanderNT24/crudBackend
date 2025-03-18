from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS  

app = Flask(__name__)
CORS(app)  # Habilitar CORS para todas las rutas

# Configuración de la base de datos SQLite
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///personas.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

# Modelo de la tabla Persona
class Persona(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(100), nullable=False)
    edad = db.Column(db.Integer, nullable=False)
    genero = db.Column(db.String(50), nullable=False)
    activo = db.Column(db.Boolean, default=True)

# Crear la base de datos y las tablas
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

@app.route("/personas/<int:id>", methods=["GET"])
def obtener_persona(id):
    persona = Persona.query.get(id)
    if not persona:
        return jsonify({"error": "Persona no encontrada"}), 404

    return jsonify({
        "id": persona.id,
        "nombre": persona.nombre,
        "edad": persona.edad,
        "genero": persona.genero,
        "activo": persona.activo
    })


# Editar una persona
@app.route("/personas/<int:id>", methods=["PUT"])
def editar_persona(id):
    persona = Persona.query.get(id)
    if not persona:
        return jsonify({"error": "Persona no encontrada"}), 404

    data = request.json
    persona.nombre = data.get("nombre", persona.nombre)
    persona.edad = data.get("edad", persona.edad)
    persona.genero = data.get("genero", persona.genero)
    persona.activo = data.get("activo", persona.activo)

    db.session.commit()
    return jsonify({"mensaje": "Persona actualizada", "persona": {
        "id": persona.id, 
        "nombre": persona.nombre, 
        "edad": persona.edad, 
        "genero": persona.genero,
        "activo": persona.activo
    }})

# Eliminar una persona
@app.route("/personas/<int:id>", methods=["DELETE"])
def eliminar_persona(id):
    persona = Persona.query.get(id)
    if not persona:
        return jsonify({"error": "Persona no encontrada"}), 404

    db.session.delete(persona)
    db.session.commit()
    
    return jsonify({"mensaje": "Persona eliminada correctamente"})

# Ejecutar el servidor
if __name__ == "__main__":
    app.run(debug=True)
