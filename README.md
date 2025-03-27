
# sat-utils-python

Repositorio de Airflow de prueba de interacción con el SAT para su servicio de descarga masiva

---

## Environment Variables

Para levantar este proyecto necesitas agregar las siguientes variables a tu **.env** file:

#### PostgreSQL
`POSTGRES_USER=airflow_sat`

`POSTGRES_PASSWORD=airflow_sat`

`POSTGRES_DB=airflow_sat`

#### Airflow - Seguridad
`AIRFLOW__CORE__FERNET_KEY=BbiODRG4PAlheSRHFkjuuhxCtPYqqYLk31bHV6E5GQA=`

`AIRFLOW__WEBSERVER__SECRET_KEY=zLyxojbYMPFqqwmE6VbBcSn7qVSx-q3l_ORV1NeKX9g=`

#### Airflow - Configuración
`AIRFLOW__CORE__EXECUTOR=LocalExecutor`

`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres/${POSTGRES_DB}`

`AIRFLOW__CORE__LOAD_EXAMPLES=false`

#### Inicialización
`_AIRFLOW_DB_INIT_FLAG=true`

`_AIRFLOW_DB_MIGRATE=true`


#### Para creación automatica de usuario admin
`_AIRFLOW_WWW_USER_CREATE=false`

`_AIRFLOW_WWW_USER_USERNAME=admin` 

`_AIRFLOW_WWW_USER_PASSWORD=admin`

`_AIRFLOW_WWW_USER_FIRSTNAME=Admin`

`_AIRFLOW_WWW_USER_LASTNAME=User`

`_AIRFLOW_WWW_USER_EMAIL=admin@example.com`

#### Credenciales de usuario admin

> [!NOTE]
> Nota airflow no deja crear usuarios personalizados con valor user:admin pass: admin

`AIRFLOW_ADMIN_USERNAME=admin_username`

`AIRFLOW_ADMIN_PASSWORD=admin_password`

`AIRFLOW_ADMIN_FIRSTNAME=admin_name`

`AIRFLOW_ADMIN_LASTNAME=admin_lastname`

`AIRFLOW_ADMIN_EMAIL=admin_correo@example.com`

Ingresa el valor que prefieras, si no ingresas este valor por defecto se asigna el valor "/demo/api": 

```python
API_PREFIX = /ruta_personalizada/api
```


---

## Installation

Descarga el proyecto

```bash
  git clone https://github.com/Santiago-Figu/sat-utils-airflow.git
  cd sat-utils-airflow
```

> [!NOTE]
> Recuerda trabajar en la rama devel

```bash
  git checkout devel
```



    
## Deployment

To deploy this project run

```bash
  docker-compose -f docker/docker-compose.yml build --no-cache
  docker-compose -f docker/docker-compose.yml up
```

Si necesitas volver a construir la imagen
```bash
  cd docker
  docker-compose down -v && docker-compose up -d
```
Si necitas crear un contenedor con etiqueta

```bash
  docker-compose -p airflow_cfdi -f docker/docker-compose.yml up -d
```

## Acknowledgements

 - [Awesome Readme Templates](https://awesomeopensource.com/project/elangosundar/awesome-README-templates)
 - [Awesome README](https://github.com/matiassingers/awesome-readme)
 - [How to write a Good readme](https://bulldogjob.com/news/449-how-to-write-a-good-readme-for-your-github-project)

- **Autor:** [Santiago Figueroa](https://github.com/Santiago-Figu)
- **Correo de contacto:** Sfigu@outlook.com
## Support

Para recibir soporte, contactar por email sfigu@outlook.com.


## Authors

- **Autor:** [Santiago Figueroa](https://github.com/Santiago-Figu)


## Feedback

If you have any feedback, please reach out to us at sfigu@outlook.com


## Documentation

[Certificados de prueba SAT](http://omawww.sat.gob.mx/tramitesyservicios/Paginas/certificado_sello_digital.htm)

[Proyecto SAT-CFDI](https://github.com/orgs/SAT-CFDI/repositories)

[cosas](https://developers.sw.com.mx/)


