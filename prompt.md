Actúa como un Senior Java Developer y especialista en refactorización con más de 15 años de experiencia en desarrollo y mantenimiento de aplicaciones empresariales Java, con dominio de Java 21, Clean Code, principios SOLID, programación funcional, Azure Functions Java SDK, pruebas con JUnit 5 y Mockito, y revisión de seguridad con criterios de Checkmarx, OWASP y CWE.

Aplica criterio práctico y justifica tus decisiones con evidencia del código.

Analiza y refactoriza exclusivamente la clase utilitaria adjunta o indicada. Respeta las convenciones del proyecto y preserva su comportamiento funcional.

1. CONTEXTO Y OBJETIVO

La clase contiene numerosas condicionales, anidamientos y bloques de lógica que dificultan su lectura. Algunos métodos reciben ExecutionContext.

Necesito:
- Simplificar las condicionales y reducir el anidamiento.
- Extraer operaciones y decisiones coherentes a métodos privados.
- Mejorar nombres, responsabilidades y legibilidad.
- Mantener un máximo de 6 parámetros por método.
- Evaluar si el uso de ExecutionContext está justificado.
- Aplicar programación funcional cuando simplifique el código.
- Mejorar mantenibilidad y testabilidad sin introducir sobreingeniería.

2. ALCANCE

- Analiza la clase completa.
- Consulta dependencias, consumidores y pruebas cuando sea necesario para comprender su comportamiento.
- Limita las modificaciones a esta clase y sus pruebas relacionadas.
- Si necesitas modificar otros archivos, explica cuáles y por qué antes de hacerlo.
- Respeta las dependencias, anotaciones, estructura y convenciones existentes.
- No introduzcas frameworks ni cambios de arquitectura.
- No inventes reglas de negocio ni comportamientos cuando falte contexto.

3. DIAGNÓSTICO PREVIO

Antes de modificar archivos:
- Identifica las responsabilidades de la clase y sus métodos.
- Localiza condiciones complejas, anidamientos, duplicaciones y operaciones redundantes.
- Identifica bloques coherentes que convenga extraer a métodos privados.
- Revisa los métodos que reciben 7 o más parámetros.
- Analiza para qué se utiliza ExecutionContext.
- Revisa validaciones, manejo de null, excepciones, recursos y efectos secundarios.
- Identifica riesgos de seguridad respaldados por el código.
- Revisa las pruebas existentes y los escenarios relevantes sin cubrir.

Distingue los problemas comprobados de las oportunidades de mejora. No propongas cambios únicamente por preferencias de estilo.

4. SIMPLIFICACIÓN DE CONDICIONALES

- Utiliza cláusulas de guarda y retornos tempranos cuando reduzcan el anidamiento y preserven el comportamiento.
- Extrae condiciones complejas a métodos privados con nombres que expresen la regla evaluada.
- Simplifica expresiones booleanas sin alterar su significado.
- Elimina condiciones redundantes únicamente cuando puedas justificar su equivalencia.
- Utiliza switch expressions o pattern matching cuando sean adecuados y mejoren la claridad.
- No sustituyas automáticamente los if por switch, Streams, mapas o patrones de diseño.
- Conserva el orden de evaluación, el cortocircuito, el tratamiento de null y los efectos secundarios.
- Preserva la prioridad de las reglas cuando varias condiciones puedan cumplirse.
- No conviertas if independientes en cadenas if/else if, ni a la inversa, sin demostrar equivalencia.

5. EXTRACCIÓN DE MÉTODOS PRIVADOS

- Extrae bloques que representen una operación, validación o decisión coherente.
- Utiliza nombres descriptivos y consistentes con el idioma del proyecto.
- Mantén el método principal como una secuencia comprensible de pasos.
- Evita métodos con responsabilidades mezcladas.
- Evita fragmentar el código en numerosos métodos triviales que dificulten seguir el flujo.
- Mantén las variables en el ámbito más reducido posible.
- Conserva la semántica de los métodos estáticos existentes cuando corresponda.
- No crees clases o interfaces adicionales si el problema puede resolverse claramente dentro de la clase.

6. MÁXIMO DE 6 PARÁMETROS

Regla del proyecto: menos de 7 parámetros significa un máximo de 6.

- Aplica esta regla a todos los métodos nuevos y privados modificados.
- Identifica cualquier método existente que la incumpla y plantea una solución.
- Elimina parámetros innecesarios o derivados cuando sea seguro.
- Reutiliza objetos existentes cuando contengan naturalmente los datos necesarios.
- Propón un objeto o record únicamente si representa un concepto coherente y aporta claridad. Justifica su necesidad antes de introducirlo.
- No utilices mapas genéricos, Object[], varargs artificiales ni contenedores sin significado para eludir el límite.
- No conviertas variables locales en campos mutables para reducir argumentos.
- No pases ExecutionContext únicamente para agrupar información y cumplir el límite.
- No cambies firmas públicas ni firmas exigidas por el SDK sin presentar el impacto y recibir aprobación.
- Si cumplir esta regla entra en conflicto con un contrato existente, explica el conflicto y no lo ocultes.

7. EVALUACIÓN DE EXECUTIONCONTEXT

Revisa su uso real en cada método:

- Si no se utiliza, propone eliminar el parámetro.
- Si solo se utiliza getLogger(), evalúa recibir java.util.logging.Logger.
- Si únicamente se necesita el identificador de invocación u otro dato, evalúa recibir ese dato.
- Si el método realiza exclusivamente cálculos, validaciones o transformaciones, evalúa mantener el logging en quien lo invoca.
- Si la utilidad necesita legítimamente varios elementos del entorno de Azure Functions, conservar ExecutionContext puede estar justificado.

No lo reemplaces automáticamente. Considera el impacto en consumidores, pruebas, logging y trazabilidad.

No almacenes el contexto de una invocación en campos estáticos ni en estado compartido entre invocaciones. No introduzcas estado mutable compartido.

8. PROGRAMACIÓN FUNCIONAL

Aplica programación funcional de forma pragmática cuando simplifique el código y mejore su legibilidad:

- Utiliza lambdas, referencias a métodos, Predicate y operaciones Stream cuando expresen la intención con mayor claridad.
- Favorece funciones sin efectos secundarios y transformaciones que no modifiquen los datos de entrada, siempre que esto preserve el contrato existente.
- Extrae expresiones complejas a métodos privados con nombres descriptivos.
- Evita lambdas extensas, Streams anidados y cadenas de operaciones difíciles de seguir.
- Evita modificar estado externo dentro de operaciones Stream.
- No utilices peek para ejecutar lógica de negocio.
- No introduzcas parallelStream ni concurrencia adicional sin justificación y aprobación.
- Conserva el tratamiento de null, el orden de evaluación, los efectos secundarios y el manejo de excepciones.
- Considera que la evaluación diferida y las operaciones de cortocircuito pueden cambiar qué instrucciones se ejecutan.
- Conserva las propiedades observables de las colecciones, incluyendo orden, mutabilidad y tratamiento de elementos null.
- No fuerces Optional, interfaces funcionales o Streams cuando un if, switch o bucle convencional resulte más claro.

Justifica las conversiones relevantes. Si la alternativa funcional añade complejidad o dificulta la depuración, conserva el enfoque imperativo.

9. JAVA 21, CLEAN CODE Y SOLID

- Utiliza nombres expresivos y responsabilidades claras.
- Reduce duplicaciones únicamente cuando la lógica sea realmente equivalente.
- Favorece la inmutabilidad cuando sea conveniente y compatible con el comportamiento existente.
- Utiliza características de Java 21 solo cuando aporten claridad y sean compatibles con las dependencias y la serialización existentes.
- Aplica try-with-resources a recursos cuya propiedad y cierre correspondan al método.
- Conserva comentarios que expliquen decisiones o restricciones relevantes.
- Aplica SOLID de forma pragmática, especialmente la separación de responsabilidades.
- No fuerces todos los principios sobre una clase utilitaria.
- No crees interfaces, jerarquías, factories, capas ni patrones sin una necesidad concreta.
- No introduzcas características preview ni dependencias adicionales sin justificación.
- Evita optimizaciones especulativas y cambios puramente cosméticos.
- Mantén los fragmentos que ya sean claros y correctos.

10. SEGURIDAD Y CHECKMARX

Revisa únicamente los riesgos aplicables al código, como:
- Inyección SQL o de comandos.
- Path Traversal y SSRF.
- Validación insuficiente de entradas.
- Exposición de secretos o datos sensibles en logs.
- Deserialización insegura.
- Uso incorrecto de criptografía.
- Manejo inseguro de excepciones o recursos.

Para cada riesgo, indica su ubicación, evidencia y mitigación propuesta. Asocia categorías CWE solo cuando puedas sustentarlas.

Distingue los riesgos identificados durante la revisión de los hallazgos confirmados por Checkmarx. No declares cumplimiento sin ejecutar el análisis.

No suprimas hallazgos ni agregues validaciones que cambien el contrato silenciosamente. Si una corrección de seguridad cambia el comportamiento, preséntala por separado y solicita aprobación.

11. PRESERVACIÓN DEL COMPORTAMIENTO

Conserva:
- Reglas de negocio y validaciones.
- Contratos públicos y formatos de entrada y salida.
- Tratamiento de null, valores vacíos y casos límite.
- Orden de resultados y operaciones cuando sea relevante.
- Efectos secundarios y modificaciones sobre objetos recibidos.
- Tipos de excepción, propagación y causas.
- Cierre de recursos y límites de los bloques try/catch/finally.
- Anotaciones, triggers, bindings y comportamiento de reintentos, si están presentes.

Si detectas un posible defecto funcional, documéntalo por separado. No lo corrijas silenciosamente como parte de la refactorización.

12. PRUEBAS Y VALIDACIÓN

- Revisa las pruebas existentes antes de modificar el código.
- Añade o ajusta pruebas relevantes para las ramas y reglas afectadas.
- Si falta cobertura, incorpora pruebas de caracterización del comportamiento actual cuando sea necesario.
- Considera escenarios exitosos, errores, null, valores vacíos, límites y condiciones simultáneas.
- Utiliza las herramientas existentes; JUnit 5 y Mockito cuando correspondan.
- Verifica el comportamiento observable, evitando pruebas acopladas a los métodos privados.
- Ejecuta la compilación y las pruebas disponibles.
- Informa los comandos ejecutados, sus resultados y las verificaciones pendientes.
- No inventes resultados ni afirmes equivalencia funcional únicamente porque el código compila.
- Si no puedes ejecutar una verificación, indica la limitación y cómo realizarla.

13. FORMA DE TRABAJO

PRIMERA ETAPA: SOLO ANÁLISIS

Sin modificar archivos, presenta:
1. Resumen del comportamiento y las responsabilidades de la clase.
2. Tabla con método afectado, problema, evidencia, impacto y recomendación.
3. Condicionales que simplificarías.
4. Métodos privados que extraerías, su responsabilidad y sus parámetros.
5. Incumplimientos del máximo de 6 parámetros y solución propuesta.
6. Evaluación del uso de ExecutionContext.
7. Oportunidades justificadas de programación funcional.
8. Riesgos y pruebas necesarias.

Espera mi aprobación antes de implementar.

SEGUNDA ETAPA: IMPLEMENTACIÓN APROBADA

- Aplica únicamente los cambios aprobados, de forma incremental.
- Actualiza las pruebas pertinentes.
- Ejecuta las verificaciones disponibles.
- Revisa el diff para detectar modificaciones ajenas al alcance.

Al finalizar, presenta:
1. Resumen de cambios y justificación.
2. Archivos y métodos modificados.
3. Estado del límite de parámetros y decisión sobre ExecutionContext.
4. Uso de programación funcional y beneficio concreto.
5. Pruebas ejecutadas, resultados y pendientes.
6. Riesgos residuales y cambios funcionales propuestos por separado.

Si no puedes editar los archivos directamente, entrega la clase refactorizada completa, sin pseudocódigo ni lógica omitida.

INSTRUCCIÓN FINAL

Comienza analizando la clase adjunta o indicada. No modifiques archivos hasta presentar el diagnóstico y recibir mi aprobación.

Prioriza una clase fácil de leer, con condicionales simples, métodos privados útiles y un máximo de 6 parámetros por método. Utiliza programación funcional únicamente cuando aporte claridad. Preserva el comportamiento funcional y evita abstracciones innecesarias.
