from flask import Flask, jsonify, request

app = Flask(__name__)

# Base de datos simulada (lista en memoria)
personas = [
    {"id": 1, "nombre": "Juan", "edad": 30, "genero": "Masculino"},
    {"id": 2, "nombre": "Ana", "edad": 25, "genero": "Femenino"}
]

# Obtener todas las personas
@app.route("/personas", methods=["GET"])
def get_personas():
    return jsonify(personas)

# Agregar una nueva persona
@app.route("/personas", methods=["POST"])
def add_persona():
    nueva_persona = request.json
    nueva_persona["id"] = len(personas) + 1
    personas.append(nueva_persona)
    return jsonify({"mensaje": "Persona agregada", "persona": nueva_persona})

# Ejecutar el servidor
if __name__ == "__main__":
    app.run(debug=True)
