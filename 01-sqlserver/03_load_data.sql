SET NOCOUNT ON;
GO

USE OHN_EHR;
GO

TRUNCATE TABLE dbo.patient;
BULK INSERT dbo.patient
FROM '/data/OHN_EHR.patient.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    MAXERRORS = 50
);
PRINT 'Loaded OHN_EHR.patient: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
GO

TRUNCATE TABLE dbo.admission;
BULK INSERT dbo.admission
FROM '/data/OHN_EHR.admission.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    MAXERRORS = 50
);
PRINT 'Loaded OHN_EHR.admission: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
GO

TRUNCATE TABLE dbo.bed_assignment;
BULK INSERT dbo.bed_assignment
FROM '/data/OHN_EHR.bed_assignment.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    MAXERRORS = 50
);
PRINT 'Loaded OHN_EHR.bed_assignment: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
GO

TRUNCATE TABLE dbo.diagnosis;
BULK INSERT dbo.diagnosis
FROM '/data/OHN_EHR.diagnosis.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    MAXERRORS = 50
);
PRINT 'Loaded OHN_EHR.diagnosis: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
GO

TRUNCATE TABLE dbo.emergency_visit;
BULK INSERT dbo.emergency_visit
FROM '/data/OHN_EHR.emergency_visit.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    MAXERRORS = 50
);
PRINT 'Loaded OHN_EHR.emergency_visit: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
GO

USE OHN_SCHED;
GO

TRUNCATE TABLE dbo.patient;
BULK INSERT dbo.patient
FROM '/data/OHN_SCHED.patient.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    MAXERRORS = 50
);
PRINT 'Loaded OHN_SCHED.patient: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
GO

TRUNCATE TABLE dbo.appointment;
BULK INSERT dbo.appointment
FROM '/data/OHN_SCHED.appointment.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    MAXERRORS = 50
);
PRINT 'Loaded OHN_SCHED.appointment: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
GO

TRUNCATE TABLE dbo.appointment_status_history;
BULK INSERT dbo.appointment_status_history
FROM '/data/OHN_SCHED.appointment_status_history.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    MAXERRORS = 50
);
PRINT 'Loaded OHN_SCHED.appointment_status_history: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
GO

USE OHN_FIN;
GO

TRUNCATE TABLE dbo.patient_account;
BULK INSERT dbo.patient_account
FROM '/data/OHN_FIN.patient_account.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    MAXERRORS = 50
);
PRINT 'Loaded OHN_FIN.patient_account: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
GO

TRUNCATE TABLE dbo.invoice;
BULK INSERT dbo.invoice
FROM '/data/OHN_FIN.invoice.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    MAXERRORS = 50
);
PRINT 'Loaded OHN_FIN.invoice: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
GO

TRUNCATE TABLE dbo.invoice_line;
BULK INSERT dbo.invoice_line
FROM '/data/OHN_FIN.invoice_line.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    MAXERRORS = 50
);
PRINT 'Loaded OHN_FIN.invoice_line: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
GO
