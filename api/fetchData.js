// api/fetchData.js

const axios = require('axios');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.SUPABASE_DB_URL,
  max: 10, // Máximo de conexiones en el pool
  idleTimeoutMillis: 30000, // Tiempo de espera antes de cerrar una conexión inactiva
});

module.exports = async (req, res) => {
  try {
    // 1. Obtener datos de la API oficial con timeout
    const apiUrl = 'https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/';
    const response = await axios.get(apiUrl, { timeout: 10000 }); // 10 segundos de timeout
    const data = response.data;

    const fechaHoy = new Date().toISOString().split('T')[0]; // YYYY-MM-DD

    // 2. Crear tabla si no existe
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS precios_gasolineras (
        id SERIAL PRIMARY KEY,
        ideess VARCHAR(50),
        rotulo VARCHAR(255),
        direccion VARCHAR(255),
        localidad VARCHAR(100),
        provincia VARCHAR(100),
        horario VARCHAR(100),
        precioGasolina95 DECIMAL(5,2),
        precioGasolina98 DECIMAL(5,2),
        precioGasoleoA DECIMAL(5,2),
        precioGasoleoPremium DECIMAL(5,2),
        precioGLP DECIMAL(5,2),
        precioGNC DECIMAL(5,2),
        precioGNL DECIMAL(5,2),
        precioHidrogeno DECIMAL(5,2),
        precioBioetanol DECIMAL(5,2),
        precioBiodiesel DECIMAL(5,2),
        precioEsterMetilico DECIMAL(5,2),
        longitud DECIMAL(10,6),
        latitud DECIMAL(10,6),
        fecha DATE
      )
    `;
    await pool.query(createTableQuery);

    // 3. Preparar datos para inserción en lote
    const values = [];
    const placeholders = [];

    data.ListaEESSPrecio.forEach((estacion, index) => {
      const baseIndex = index * 20 + 1; // Número de campos

      placeholders.push(`(${Array.from({ length: 20 }, (_, i) => `$${baseIndex + i}`).join(',')})`);

      values.push(
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
        fechaHoy
      );
    });

    if (values.length > 0) {
      const insertQuery = `
        INSERT INTO precios_gasolineras (
          ideess, rotulo, direccion, localidad, provincia, horario,
          precioGasolina95, precioGasolina98, precioGasoleoA, precioGasoleoPremium,
          precioGLP, precioGNC, precioGNL, precioHidrogeno,
          precioBioetanol, precioBiodiesel, precioEsterMetilico,
          longitud, latitud, fecha
        ) VALUES ${placeholders.join(',')}
      `;
      await pool.query(insertQuery, values);
    }

    res.status(200).json({ message: 'Datos obtenidos y almacenados correctamente.' });
  } catch (error) {
    console.error('Error en fetchData:', error);
    res.status(500).json({ error: 'Ocurrió un error al procesar la solicitud.' });
  }
};