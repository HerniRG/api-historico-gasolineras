// api/fetchData.js

const axios = require('axios');
const { Pool } = require('pg');

// Crear una instancia de Pool para manejar conexiones a la base de datos
const pool = new Pool({
  connectionString: process.env.SUPABASE_DB_URL,
});

module.exports = async (req, res) => {
  try {
    // 1. Obtener datos de la API oficial
    const apiUrl = 'https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/';
    const response = await axios.get(apiUrl);
    const data = response.data;

    // 2. Insertar datos en la base de datos
    const insertQuery = `
      INSERT INTO precios_gasolineras (
        ideess, rotulo, direccion, localidad, provincia, horario,
        precioGasolina95, precioGasolina98, precioGasoleoA, precioGasoleoPremium,
        precioGLP, precioGNC, precioGNL, precioHidrogeno,
        precioBioetanol, precioBiodiesel, precioEsterMetilico,
        longitud, latitud, fecha
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
    `;

    const fechaHoy = new Date().toISOString().split('T')[0]; // YYYY-MM-DD

    // Utilizar una transacción para mejorar la integridad de los datos
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const estacion of data.ListaEESSPrecio) {
        await client.query(insertQuery, [
          estacion.IDEESS,
          estacion.Rótulo,
          estacion.Dirección,
          estacion.Localidad,
          estacion.Provincia,
          estacion.Horario,
          estacion.PrecioGasolina95E5,
          estacion.PrecioGasolina98E5,
          estacion.PrecioGasoleoA,
          estacion.PrecioGasoleoPremium,
          estacion.PrecioGLP,
          estacion.PrecioGNC,
          estacion.PrecioGNL || null,
          estacion.PrecioHidrogeno || null,
          estacion.PrecioBioetanol || null,
          estacion.PrecioBiodiesel || null,
          estacion.PrecioEsterMetilico || null,
          estacion.LongitudWGS84,
          estacion.Latitud,
          fechaHoy,
        ]);
      }
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }

    res.status(200).json({ message: 'Datos obtenidos y almacenados correctamente.' });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Ocurrió un error al procesar la solicitud.' });
  }
};
