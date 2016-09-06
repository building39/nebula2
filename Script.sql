--<ScriptOptions statementTerminator="GO"/>

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE VIEW dbo.InternCertificationTypes
AS
SELECT DISTINCT InternID, CertificationTypeID, InternCertificationID AS id
FROM            dbo.InternCertification
WHERE        (CertificationTypeID NOT IN (1, 22))
GO

SET ANSI_PADDING OFF
GO

