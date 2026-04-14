# CUENTASMEDICAS_SIIFA

## Positiva Compañía de Seguros

### Vicepresidencia Técnica

---

## 1. Descripción General

Este proyecto implementa un servicio backend desplegado en Google Cloud Run, diseñado para el procesamiento de cuentas médicas, generación de archivos Excel y almacenamiento en Google Cloud Storage.

La solución está construida bajo un enfoque serverless, con integración continua y despliegue automático mediante GitHub Actions, utilizando autenticación segura basada en Workload Identity Federation (WIF).

---

## 2. Objetivos

* Automatizar el procesamiento de cuentas médicas
* Eliminar el uso de credenciales estáticas
* Implementar despliegues continuos seguros
* Garantizar escalabilidad y alta disponibilidad
* Establecer un modelo replicable para futuros proyectos

---

## 3. Arquitectura de la Solución

### 3.1 Arquitectura Lógica

Código fuente → GitHub → GitHub Actions → WIF → Cloud Build → Cloud Run → Cloud Storage






### 3.2 Componentes

* Control de código fuente: GitHub
* CI/CD: GitHub Actions
* Identidad: Workload Identity Federation
* Cómputo: Cloud Run
* Construcción: Cloud Build
* Almacenamiento: Cloud Storage

---

## 4. Modelo de Despliegue

### 4.1 Estrategia de Despliegue

* Tipo: Despliegue continuo
* Disparador: Push a rama main
* Frecuencia: Automática
* Intervención manual: No requerida

---

### 4.2 Flujo de Despliegue

1. Commit y push al repositorio
2. Ejecución de GitHub Actions
3. Autenticación mediante OIDC
4. Construcción de imagen
5. Despliegue en Cloud Run
6. Publicación de nueva revisión

---

## 5. Modelo de Identidad y Seguridad

### 5.1 Autenticación

Se utiliza Workload Identity Federation para permitir autenticación sin credenciales.

Configuración:

* Workload Identity Pool: github-pool
* Provider: github-provider

Restricción:

assertion.repository == "juancortespositiva/cuentasmedicas-siifa"

---

### 5.2 Autorización

Cuenta de servicio:

[github-actions-sa@analitica-contact-center-dev.iam.gserviceaccount.com](mailto:github-actions-sa@analitica-contact-center-dev.iam.gserviceaccount.com)

Roles:

Sugeridos
- roles/run.developer  
- roles/artifactregistry.writer  
- roles/cloudbuild.builds.editor  
- roles/logging.viewer  
- roles/iam.serviceAccountUser  
- roles/iam.workloadIdentityUser  

* Permisos de ejecucion de Cloud Run
* Escritor de Artifact Registry
* Editor de Cloud Build
* Visualizador de logs
* Usuario de cuenta de servicio
* Usuario de Workload Identity

---

### 5.3 Principios de Seguridad

* Eliminación de secretos en código
* Autenticación basada en identidad
* Control de acceso por repositorio
* Principio de menor privilegio

---

## 6. Diseño de Infraestructura

### 6.1 Cloud Run

* Servicio: cuentasmedicas-siifa
* Región: us-central1
* Escalamiento: automático
* Acceso: público

---

### 6.2 Cloud Storage

* Uso: almacenamiento de archivos generados
* Tipo: almacenamiento de objetos
* Persistencia: alta durabilidad

---

### 6.3 Cloud Build

* Construcción de imagen de contenedor
* Integración con pipeline CI/CD

---

## 7. Diseño del Pipeline CI/CD

Archivo:

.github/workflows/deploy.yml

### Etapas:

1. Obtención del código (checkout)
2. Autenticación con Google Cloud
3. Configuración de entorno
4. Construcción
5. Despliegue

---

### 7.1 Características del Pipeline

* Idempotente
* Automatizado
* Sin manejo de credenciales
* Integrado con IAM

---

## 8. Modelo Operativo

### 8.1 Despliegue

Automático mediante:

git add .
git commit -m "mensaje"
git push

---

### 8.2 Despliegue Manual (Contingencia)

gcloud run deploy cuentasmedicas-siifa --source . --region us-central1

---

### 8.3 Monitoreo

* Cloud Logging
* Logs de Cloud Run
* Logs de GitHub Actions

---

## 9. Confiabilidad y Escalabilidad

* Escalamiento automático de Cloud Run
* Despliegue basado en revisiones
* Capacidad de rollback inmediato
* Alta disponibilidad regional

---

## 10. Observabilidad

* Logs centralizados en Cloud Logging
* Trazabilidad de despliegues en GitHub
* Seguimiento de ejecuciones en Cloud Run

---

## 11. Versionamiento y Rollback

* Cada despliegue genera una nueva revisión
* Rollback disponible desde consola GCP
* Versionamiento implícito por imagen

---

## 12. Optimización de Costos

* Modelo serverless (pago por uso)
* Escalamiento a cero cuando no hay tráfico
* Eliminación de infraestructura ociosa

---

## 13. Riesgos y Mitigaciones

| Riesgo                     | Mitigación                  |
| -------------------------- | --------------------------- |
| Acceso no autorizado       | Restricción por repositorio |
| Exposición de credenciales | Uso de WIF                  |
| Fallos en despliegue       | Pipeline automatizado       |
| Errores en código          | Control de versiones        |

---

## 14. Buenas Prácticas

* No utilizar credenciales estáticas
* Mantener condiciones de seguridad en WIF
* Validar cambios antes de push
* Monitorear logs continuamente
* Usar ramas para cambios críticos

---

## 15. Mejoras Futuras

* Separación de entornos (desarrollo, pruebas, producción)
* Integración con Secret Manager
* Implementación de pruebas automáticas
* Observabilidad avanzada

---

## 16. Gobierno

Este proyecto sigue lineamientos de:

* Seguridad en la nube
* Automatización de despliegues
* Arquitectura serverless
* Control de acceso basado en identidad

---

## 17. Responsabilidad

Positiva Compañía de Seguros
Vicepresidencia Técnica

---

## 18. Conclusión

La solución implementada establece un estándar moderno para despliegues en la nube:

* Seguro
* Automatizado
* Escalable
* Reproducible

Se recomienda su adopción como modelo base para futuros desarrollos en la organización.

---

