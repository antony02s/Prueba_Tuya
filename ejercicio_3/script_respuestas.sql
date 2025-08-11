-- CREACION DE TABLA RACHAS
create table clientes_rachas (
	id serial primary key,
	identificacion text,
	corte_mes date,
	saldo numeric(12,2)
	
);

-- CREACION DE TABLA RETIROS
create table  retiros (
	id serial primary key,
--RACHA MAYOR POR CLIENTE
	identificacion text,
	fecha_retiro date
);

-- NOTA: La carga de datos se realizó a través de importación del archivo .CSV

-- CLASIFICACION POR NIVEL DE DEUDA (TENIENDO EN CUENTA LA CONTINUIDAD DEL CLIENTE DENTRO DE LAS FECHAS DE CORTE Y FECHA DE RETIRO)
with clientes as (
	select distinct c.identificacion from clientes_rachas as c
),
cortes AS (
  SELECT DISTINCT corte_mes
  FROM clientes_rachas
),
-- Calculo de niveles de deuda
niveles_deuda as (SELECT
  c.identificacion,
  co.corte_mes,
  cr.saldo,
  case
  	when cr.saldo is null then 'n0'
  	when cr.saldo>=0 and cr.saldo<300000 then 'n0'
  	when cr.saldo>=300000 and cr.saldo<1000000 then 'n1' 
  	when cr.saldo>=1000000 and cr.saldo<3000000 then 'n2' 
  	when cr.saldo>=3000000 and cr.saldo<5000000 then 'n3' 
  	when cr.saldo>=5000000 then 'n4' 
  end as nivel_deuda
FROM clientes c
CROSS JOIN cortes co
LEFT JOIN clientes_rachas cr
  ON cr.identificacion = c.identificacion
 AND cr.corte_mes = co.corte_mes
ORDER BY c.identificacion, co.corte_mes
),
-- Identificar los meses consecutivos (racha) de un cliente dentro de un nivel de saldo.
base AS (
  SELECT
    identificacion  AS cliente_id,
    date_trunc('month', corte_mes)::date AS corte_mes,
    nivel_deuda
  FROM niveles_deuda
),
marcado AS (
  SELECT
    cliente_id,
    corte_mes,
    nivel_deuda,
    LAG(corte_mes)   OVER (PARTITION BY cliente_id ORDER BY corte_mes) AS corte_prev,
    LAG(nivel_deuda) OVER (PARTITION BY cliente_id ORDER BY corte_mes) AS nivel_prev
  FROM base
),
grupos AS (
  SELECT
    cliente_id,
    corte_mes,
    nivel_deuda,
    SUM(
      CASE
        WHEN nivel_deuda IS DISTINCT FROM nivel_prev
          OR corte_prev <> (corte_mes - INTERVAL '1 month')
        THEN 1 ELSE 0
      END
    ) OVER (PARTITION BY cliente_id ORDER BY corte_mes) AS racha_id
  FROM marcado
),
-- Calcular las rachas que cumplan con la condicion de igualdad de nivel de deuda por al menos dos meses
rachas AS (
  SELECT
    cliente_id,
    nivel_deuda,
    MIN(corte_mes) AS inicio_racha,
    MAX(corte_mes) AS fin_racha,
    COUNT(*)       AS meses_consecutivos
  FROM grupos
  GROUP BY cliente_id, nivel_deuda, racha_id
  HAVING COUNT(*) >= 2
),
-- Seleccionar las rachas mayores a dos mas recientes por cliente
ranked AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER (
      PARTITION BY cliente_id
      ORDER BY fin_racha DESC
    ) AS rn
  FROM rachas r
)
SELECT
  cliente_id,
  nivel_deuda,
  inicio_racha,
  fin_racha,
  meses_consecutivos
FROM ranked
WHERE rn = 1
ORDER BY cliente_id;
