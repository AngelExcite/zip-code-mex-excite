const fs = require("fs");
const csv = require("csv-parser");
const MongoClient = require("mongodb").MongoClient;

const url = "mongodb://localhost:27017";
const dbName = "codigos_postales";

const estados = {};

// Limpiamos el archivo de ingesta, pasando de windows-latin-1 a utf8 y quitando la primera línea
// Nota: Asegúrate de tener el archivo CPdescarga.txt en la misma carpeta que este script.

const { exec } = require("child_process");
exec(
  'iconv -f ISO-8859-15 -t UTF-8 CPdescarga.txt | sed "1d" > CPdescarga-utf8.txt',
  (err, stdout, stderr) => {
    if (err) {
      console.error("Error al limpiar el archivo:", err);
      return;
    }

    console.log("Archivo limpiado y convertido a UTF-8.");
    processFile();
  }
);

function processFile() {
  fs.createReadStream("CPdescarga-utf8.txt")
    .pipe(csv({ headers: true, separator: "|", quote: "|" }))
    .on("data", (row) => {
      if (row.length > 15) {
        console.log(row);
        return;
      }

      const estado = parseInt(row["c_estado"]);

      const doc = {
        _id: `${row["d_codigo"]}-${row["id_asenta_cpcons"]}`,
        cp: row["d_codigo"],
        estado: estado,
        municipio: row["D_mnpio"].trim(),
        colonia: row["d_asenta"].trim(),
        tipo: row["d_tipo_asenta"].toLowerCase().trim(),
        zona: row["d_zona"].toLowerCase().trim(),
      };

      if (row["d_ciudad"]) {
        doc.ciudad = row["d_ciudad"].trim();
      }

      if (!estados.hasOwnProperty(estado)) {
        console.log(`Agregando ${row["d_estado"]}: ${row["c_estado"]}`);
        estados[estado] = row["d_estado"].trim();
        saveEstado(estado, row["d_estado"].trim());
      }

      saveCP(doc);
    })
    .on("end", () => {
      console.log("Proceso completado.");
    })
    .on("error", (err) => {
      console.error("Error al procesar el archivo:", err);
    });
}

function saveEstado(estadoId, estadoNombre) {
  MongoClient.connect(url, function (err, client) {
    if (err) {
      console.error("Error al conectar a MongoDB:", err);
      return;
    }

    const db = client.db(dbName);
    db.collection("estados").save(
      { _id: estadoId, nombre: estadoNombre },
      function (err, result) {
        if (err) {
          console.error("Error al guardar estado en MongoDB:", err);
        }

        client.close();
      }
    );
  });
}

function saveCP(doc) {
  MongoClient.connect(url, function (err, client) {
    if (err) {
      console.error("Error al conectar a MongoDB:", err);
      return;
    }

    const db = client.db(dbName);
    db.collection("cp").save(doc, function (err, result) {
      if (err) {
        console.error("Error al guardar código postal en MongoDB:", err);
      }

      client.close();
    });
  });
}
