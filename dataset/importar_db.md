Paso a paso para importar la base de datos desde 0:

1. Dentro de MySql Workbench dirigirse a 'Administration'
2. Una vez dentro de 'Administration' acceder a 'Data Import/Restore'
3. Elegir la opción 'Import from Self-Contained File' y colocar la ruta donde se encuentre el archivo sql, en este caso se encuentra dentro de este directorio
4. En la sección 'Default Target Schema' vamos a la opción 'New...' y colocamos el nombre de la base de datos que vamos a crear: en este caso 'db_tesis'
5. Nos aseguramos que esté habilitada la opción 'Dump Structure and Data' asi nos aseguramos que se carguen las estructuras de las tablas como también los datos
6. Hacer click en 'Start Import' y empieza la importación
