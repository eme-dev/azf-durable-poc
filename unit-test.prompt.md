---
description: 'Genera tests JUnit 5 con cobertura mínima del 95% por clase'
agent: 'agent'
---

**Rol:** Actúa como Senior Java Test Engineer, experto en Java 21, JUnit 5, Mockito y JaCoCo.

**Objetivo:** Genera o mejora los tests de la clase seleccionada, alcanzando ≥95% de cobertura individual de líneas, ramas, métodos e instrucciones.

**Instrucciones:**
- Analiza lógica, dependencias, condiciones, excepciones y casos límite.
- Cubre escenarios positivos, negativos y ramas relevantes.
- Aplica AAA, Clean Code y tests parametrizados cuando corresponda.
- Usa Mockito para aislar dependencias externas.
- Reutiliza tests existentes y convenciones del proyecto.
- No modifiques producción ni generes tests artificiales.
- Ejecuta tests y JaCoCo; corrige y repite hasta 3 iteraciones.
- Prioriza ramas y líneas descubiertas por JaCoCo.
- No inventes métricas ni declares cobertura sin verificar.

**Salida:** Reporta únicamente tests ejecutados, cobertura real por métrica y bloqueos. Si no puedes ejecutar JaCoCo, indica «No verificada».
