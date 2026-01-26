/*==============================================================================
  SQL Server 2019 - Limpieza de duplicados (mantener el último) + Respaldo + UNIQUE
  Tabla objetivo: [ocrt].[EstructuracionEncrypt]
  Clave lógica (debe ser única): fileName
  Regla para “último”: creationDateTime más reciente (si empata, mayor ID)

  Este script crea:
    1) Esquema y tabla de prueba (igual a tu estructura, con fileName NVARCHAR(MAX))
    2) Data de prueba con duplicados
    3) Respaldo de registros que serán eliminados (SELECT INTO)
    4) Eliminación de duplicados manteniendo el último
    5) Enforce a futuro: UNIQUE(fileName)

  Nota importante:
    - SQL Server NO permite UNIQUE/INDEX sobre NVARCHAR(MAX).
    - Por eso, después de limpiar, se altera fileName a NVARCHAR(450) y recién se crea UNIQUE.
==============================================================================*/

SET NOCOUNT ON;
GO

/*==============================================================================
  0) Preparación: crear esquema si no existe
==============================================================================*/
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ocrt')
    EXEC('CREATE SCHEMA [ocrt]');
GO

/*==============================================================================
  1) Crear tabla de prueba (drop & create para que sea reproducible)
==============================================================================*/
IF OBJECT_ID('[ocrt].[EstructuracionEncrypt]', 'U') IS NOT NULL
    DROP TABLE [ocrt].[EstructuracionEncrypt];
GO

CREATE TABLE [ocrt].[EstructuracionEncrypt] (
    [ID]               INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_EstructuracionEncrypt PRIMARY KEY,
    [fileName]         NVARCHAR(MAX) NOT NULL,      -- (en tu tabla real está MAX)
    [statusFile]       BIT NOT NULL,
    [clientName]       NVARCHAR(MAX) NULL,
    [creationDateTime] DATETIME NULL,               -- define “último”
    [dataMap]          NVARCHAR(MAX) NULL,
    [content]          NVARCHAR(MAX) NULL,
    [listaTables]      NVARCHAR(MAX) NULL
);
GO

/*==============================================================================
  2) Insertar data de prueba (con duplicados por fileName)
     - Para fileA habrá 3 registros -> debe quedar el de 2026-01-03
     - Para fileB habrá 2 registros -> debe quedar el de 2026-01-05
     - Para fileC habrá 1 registro -> se queda tal cual
==============================================================================*/
INSERT INTO [ocrt].[EstructuracionEncrypt]
    ([fileName], [statusFile], [clientName], [creationDateTime], [dataMap], [content], [listaTables])
VALUES
    (N'fileA.pdf', 1, N'Cliente 1', '2026-01-01T10:00:00', N'{}', N'contenido A1', N'tabla1'),
    (N'fileA.pdf', 1, N'Cliente 1', '2026-01-02T10:00:00', N'{}', N'contenido A2', N'tabla1'),
    (N'fileA.pdf', 1, N'Cliente 1', '2026-01-03T10:00:00', N'{}', N'contenido A3 (último)', N'tabla1'),

    (N'fileB.pdf', 0, N'Cliente 2', '2026-01-04T09:00:00', N'{}', N'contenido B1', N'tabla2'),
    (N'fileB.pdf', 1, N'Cliente 2', '2026-01-05T09:00:00', N'{}', N'contenido B2 (último)', N'tabla2'),

    (N'fileC.pdf', 1, N'Cliente 3', '2026-01-06T08:00:00', N'{}', N'contenido C', N'tabla3');
GO

/*==============================================================================
  3) Ver estado ANTES (para comprobar duplicados)
==============================================================================*/
PRINT 'ANTES:';
SELECT [ID], [fileName], [creationDateTime], [statusFile], [clientName]
FROM [ocrt].[EstructuracionEncrypt]
ORDER BY [fileName], [creationDateTime], [ID];
GO

/*==============================================================================
  4) Limpieza: Respaldo + Eliminación de duplicados (mantener el último)
==============================================================================*/

-- 4.1) Validación previa para poder crear UNIQUE luego:
--      Como fileName es NVARCHAR(MAX), después lo bajaremos a NVARCHAR(450).
--      Esto exige que no exista ningún fileName > 450 caracteres.
IF EXISTS (
    SELECT 1
    FROM [ocrt].[EstructuracionEncrypt]
    WHERE LEN([fileName]) > 450
)
BEGIN
    THROW 50010, 'Existen fileName con longitud > 450. No se puede aplicar UNIQUE luego. Ajusta estrategia o tamaño.', 1;
END
GO

BEGIN TRAN;

    /* 4.2) Crear tabla de respaldo SOLO con filas que serán eliminadas */
    IF OBJECT_ID('[ocrt].[EstructuracionEncrypt_Backup_OneShot]', 'U') IS NOT NULL
        DROP TABLE [ocrt].[EstructuracionEncrypt_Backup_OneShot];

    ;WITH x AS
    (
        SELECT *,
               rn = ROW_NUMBER() OVER
                    (
                      PARTITION BY [fileName]
                      ORDER BY [creationDateTime] DESC, [ID] DESC
                    )
        FROM [ocrt].[EstructuracionEncrypt]
    )
    SELECT *
    INTO [ocrt].[EstructuracionEncrypt_Backup_OneShot]
    FROM x
    WHERE rn > 1;

    /* 4.3) Eliminar duplicados dejando el último (rn = 1 se queda) */
    ;WITH x AS
    (
        SELECT *,
               rn = ROW_NUMBER() OVER
                    (
                      PARTITION BY [fileName]
                      ORDER BY [creationDateTime] DESC, [ID] DESC
                    )
        FROM [ocrt].[EstructuracionEncrypt]
    )
    DELETE FROM x
    WHERE rn > 1;

COMMIT TRAN;
GO

/*==============================================================================
  5) Ver estado DESPUÉS (solo debe quedar 1 fila por fileName)
==============================================================================*/
PRINT 'DESPUES (limpio):';
SELECT [ID], [fileName], [creationDateTime], [statusFile], [clientName]
FROM [ocrt].[EstructuracionEncrypt]
ORDER BY [fileName], [creationDateTime], [ID];
GO

PRINT 'RESPALDO (lo eliminado):';
SELECT [ID], [fileName], [creationDateTime], [statusFile], [clientName], rn
FROM [ocrt].[EstructuracionEncrypt_Backup_OneShot]
ORDER BY [fileName], [creationDateTime], [ID];
GO

/*==============================================================================
  6) Enforce a futuro: UNIQUE(fileName)
     - Primero debemos convertir fileName de NVARCHAR(MAX) a NVARCHAR(450)
==============================================================================*/
ALTER TABLE [ocrt].[EstructuracionEncrypt]
ALTER COLUMN [fileName] NVARCHAR(450) NOT NULL;
GO

ALTER TABLE [ocrt].[EstructuracionEncrypt]
ADD CONSTRAINT UQ_EstructuracionEncrypt_fileName UNIQUE ([fileName]);
GO

/*==============================================================================
  7) Prueba de que ya NO permite duplicados
==============================================================================*/
PRINT 'PRUEBA: intentar insertar duplicado (debe FALLAR por UNIQUE):';
BEGIN TRY
    INSERT INTO [ocrt].[EstructuracionEncrypt]
        ([fileName], [statusFile], [clientName], [creationDateTime], [dataMap], [content], [listaTables])
    VALUES
        (N'fileA.pdf', 1, N'Cliente X', '2026-01-10T10:00:00', N'{}', N'no debería insertar', N'tablaX');
END TRY
BEGIN CATCH
    SELECT
        ErrorNumber  = ERROR_NUMBER(),
        ErrorMessage = ERROR_MESSAGE();
END CATCH;
GO
