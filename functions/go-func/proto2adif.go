package forester

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/farmergreg/adif/v5"
	"github.com/farmergreg/spec/v6/adifield"
	adifpb "github.com/k0swe/adif-json-protobuf/go"
)

func protoToAdif(pb *adifpb.Adif) (string, error) {
	buf := new(bytes.Buffer)
	writer := adif.NewADIDocumentWriter(buf)
	for _, qso := range pb.Qsos {
		record := writeQso(qso)
		err := writer.WriteRecord(record)
		if err != nil {
			return "", err
		}
	}
	err := writer.Flush()
	return buf.String(), err
}

func writeQso(qso *adifpb.Qso) adif.Record {
	var rec = adif.NewRecord()
	writeTopLevel(qso, rec)
	writeAppDefined(qso, rec)
	writeContactedStation(qso, rec)
	writeLoggingStation(qso, rec)
	writeContest(qso, rec)
	writePropagation(qso, rec)
	writeAwardsAndCredit(qso, rec)
	writeUploads(qso, rec)
	writeQsls(qso, rec)
	return rec
}

func writeTopLevel(qso *adifpb.Qso, rec adif.Record) {
	writeString(rec, adifield.BAND, qso.Band)
	writeString(rec, adifield.BAND_RX, qso.BandRx)
	writeString(rec, adifield.COMMENT, qso.Comment)
	writeInt(rec, adifield.DISTANCE, int64(qso.DistanceKm))
	writeFloat(rec, adifield.FREQ, qso.Freq, 6)
	writeFloat(rec, adifield.FREQ_RX, qso.FreqRx, 6)
	writeString(rec, adifield.MODE, qso.Mode)
	writeString(rec, adifield.NOTES, qso.Notes)
	writeString(rec, adifield.PUBLIC_KEY, qso.PublicKey)
	writeString(rec, adifield.QSO_COMPLETE, qso.Complete)
	writeDate(rec, adifield.QSO_DATE, qso.TimeOn.AsTime())
	writeTime(rec, adifield.TIME_ON, qso.TimeOn.AsTime())
	writeDate(rec, adifield.QSO_DATE_OFF, qso.TimeOff.AsTime())
	writeTime(rec, adifield.TIME_OFF, qso.TimeOff.AsTime())
	writeBool(rec, adifield.QSO_RANDOM, qso.Random)
	writeString(rec, adifield.RST_RCVD, qso.RstReceived)
	writeString(rec, adifield.RST_SENT, qso.RstSent)
	writeString(rec, adifield.SUBMODE, qso.Submode)
	writeBool(rec, adifield.SWL, qso.Swl)
}

func writeAppDefined(qso *adifpb.Qso, rec adif.Record) {
	for k, v := range qso.AppDefined {
		rec.Set(adifield.New(k), v)
	}
}

func writeContactedStation(qso *adifpb.Qso, rec adif.Record) {
	if qso.ContactedStation == nil {
		return
	}
	writeString(rec, adifield.ADDRESS, qso.ContactedStation.Address)
	writeInt(rec, adifield.AGE, int64(qso.ContactedStation.Age))
	writeString(rec, adifield.CALL, qso.ContactedStation.StationCall)
	writeString(rec, adifield.CNTY, qso.ContactedStation.County)
	writeString(rec, adifield.CONT, qso.ContactedStation.Continent)
	writeString(rec, adifield.CONTACTED_OP, qso.ContactedStation.OpCall)
	writeString(rec, adifield.COUNTRY, qso.ContactedStation.Country)
	writeInt(rec, adifield.CQZ, int64(qso.ContactedStation.CqZone))
	writeString(rec, adifield.DARC_DOK, qso.ContactedStation.DarcDok)
	writeInt(rec, adifield.DXCC, int64(qso.ContactedStation.Dxcc))
	writeString(rec, adifield.EMAIL, qso.ContactedStation.Email)
	writeString(rec, adifield.EQ_CALL, qso.ContactedStation.OwnerCall)
	writeInt(rec, adifield.FISTS, int64(qso.ContactedStation.Fists))
	writeInt(rec, adifield.FISTS_CC, int64(qso.ContactedStation.FistsCc))
	writeString(rec, adifield.GRIDSQUARE, qso.ContactedStation.GridSquare)
	writeString(rec, adifield.IOTA, qso.ContactedStation.Iota)
	writeInt(rec, adifield.IOTA_ISLAND_ID, int64(qso.ContactedStation.IotaIslandId))
	writeInt(rec, adifield.ITUZ, int64(qso.ContactedStation.ItuZone))
	writeLatLon(rec, adifield.LAT, qso.ContactedStation.Latitude, true)
	writeLatLon(rec, adifield.LON, qso.ContactedStation.Longitude, false)
	writeString(rec, adifield.NAME, qso.ContactedStation.OpName)
	writeString(rec, adifield.PFX, qso.ContactedStation.Pfx)
	writeString(rec, adifield.QSL_VIA, qso.ContactedStation.QslVia)
	writeString(rec, adifield.QTH, qso.ContactedStation.City)
	writeString(rec, adifield.REGION, qso.ContactedStation.Region)
	writeString(rec, adifield.RIG, qso.ContactedStation.Rig)
	writeFloat(rec, adifield.RX_PWR, qso.ContactedStation.Power, 2)
	writeString(rec, adifield.SIG, qso.ContactedStation.Sig)
	writeString(rec, adifield.SIG_INFO, qso.ContactedStation.SigInfo)
	writeBool(rec, adifield.SILENT_KEY, qso.ContactedStation.SilentKey)
	writeString(rec, adifield.SKCC, qso.ContactedStation.Skcc)
	writeString(rec, adifield.SOTA_REF, qso.ContactedStation.SotaRef)
	writeString(rec, adifield.STATE, qso.ContactedStation.State)
	writeInt(rec, adifield.TEN_TEN, int64(qso.ContactedStation.TenTen))
	writeInt(rec, adifield.UKSMG, int64(qso.ContactedStation.Uksmg))
	writeString(rec, adifield.USACA_COUNTIES, qso.ContactedStation.UsacaCounties)
	writeString(rec, adifield.VUCC_GRIDS, qso.ContactedStation.VuccGrids)
	writeString(rec, adifield.WEB, qso.ContactedStation.Web)
}

func writeLoggingStation(qso *adifpb.Qso, rec adif.Record) {
	if qso.LoggingStation == nil {
		return
	}
	writeInt(rec, adifield.ANT_AZ, int64(qso.LoggingStation.AntennaAzimuth))
	writeInt(rec, adifield.ANT_EL, int64(qso.LoggingStation.AntennaElevation))
	writeString(rec, adifield.MY_ANTENNA, qso.LoggingStation.Antenna)
	writeString(rec, adifield.MY_CITY, qso.LoggingStation.City)
	writeString(rec, adifield.MY_CNTY, qso.LoggingStation.County)
	writeString(rec, adifield.MY_COUNTRY, qso.LoggingStation.Country)
	writeInt(rec, adifield.MY_CQ_ZONE, int64(qso.LoggingStation.CqZone))
	writeInt(rec, adifield.MY_DXCC, int64(qso.LoggingStation.Dxcc))
	writeInt(rec, adifield.MY_FISTS, int64(qso.LoggingStation.Fists))
	writeString(rec, adifield.MY_GRIDSQUARE, qso.LoggingStation.GridSquare)
	writeString(rec, adifield.MY_IOTA, qso.LoggingStation.Iota)
	writeInt(rec, adifield.MY_IOTA_ISLAND_ID, int64(qso.LoggingStation.IotaIslandId))
	writeInt(rec, adifield.MY_ITU_ZONE, int64(qso.LoggingStation.ItuZone))
	writeLatLon(rec, adifield.MY_LAT, qso.LoggingStation.Latitude, true)
	writeLatLon(rec, adifield.MY_LON, qso.LoggingStation.Longitude, false)
	writeString(rec, adifield.MY_NAME, qso.LoggingStation.OpName)
	writeString(rec, adifield.MY_POSTAL_CODE, qso.LoggingStation.PostalCode)
	writeString(rec, adifield.MY_RIG, qso.LoggingStation.Rig)
	writeString(rec, adifield.MY_SIG, qso.LoggingStation.Sig)
	writeString(rec, adifield.MY_SIG_INFO, qso.LoggingStation.SigInfo)
	writeString(rec, adifield.MY_SOTA_REF, qso.LoggingStation.SotaRef)
	writeString(rec, adifield.MY_STATE, qso.LoggingStation.State)
	writeString(rec, adifield.MY_STREET, qso.LoggingStation.Street)
	writeString(rec, adifield.MY_USACA_COUNTIES, qso.LoggingStation.UsacaCounties)
	writeString(rec, adifield.MY_VUCC_GRIDS, qso.LoggingStation.VuccGrids)
	writeString(rec, adifield.OPERATOR, qso.LoggingStation.OpCall)
	writeString(rec, adifield.OWNER_CALLSIGN, qso.LoggingStation.OwnerCall)
	writeString(rec, adifield.STATION_CALLSIGN, qso.LoggingStation.StationCall)
	writeFloat(rec, adifield.TX_PWR, qso.LoggingStation.Power, 2)
}

func writeContest(qso *adifpb.Qso, rec adif.Record) {
	if qso.Contest == nil {
		return
	}
	writeString(rec, adifield.CONTEST_ID, qso.Contest.ContestId)
	writeString(rec, adifield.ARRL_SECT, qso.Contest.ArrlSection)
	writeString(rec, adifield.CLASS, qso.Contest.StationClass)
	writeString(rec, adifield.CHECK, qso.Contest.Check)
	writeString(rec, adifield.PRECEDENCE, qso.Contest.Precedence)
	writeString(rec, adifield.SRX, qso.Contest.SerialReceived)
	writeString(rec, adifield.SRX_STRING, qso.Contest.SerialReceived)
	writeString(rec, adifield.STX, qso.Contest.SerialSent)
	writeString(rec, adifield.STX_STRING, qso.Contest.SerialSent)
}

func writePropagation(qso *adifpb.Qso, rec adif.Record) {
	if qso.Propagation == nil {
		return
	}
	writeInt(rec, adifield.A_INDEX, int64(qso.Propagation.AIndex))
	writeString(rec, adifield.ANT_PATH, qso.Propagation.AntPath)
	writeBool(rec, adifield.FORCE_INIT, qso.Propagation.ForceInit)
	writeInt(rec, adifield.K_INDEX, int64(qso.Propagation.KIndex))
	writeInt(rec, adifield.MAX_BURSTS, int64(qso.Propagation.MaxBursts))
	writeString(rec, adifield.MS_SHOWER, qso.Propagation.MeteorShowerName)
	writeInt(rec, adifield.NR_BURSTS, int64(qso.Propagation.NrBursts))
	writeInt(rec, adifield.NR_PINGS, int64(qso.Propagation.NrPings))
	writeString(rec, adifield.PROP_MODE, qso.Propagation.PropagationMode)
	writeString(rec, adifield.SAT_MODE, qso.Propagation.SatMode)
	writeString(rec, adifield.SAT_NAME, qso.Propagation.SatName)
	writeInt(rec, adifield.SFI, int64(qso.Propagation.SolarFluxIndex))
}

func writeAwardsAndCredit(qso *adifpb.Qso, rec adif.Record) {
	writeAwards(qso.AwardSubmitted, rec, adifield.AWARD_SUBMITTED)
	writeAwards(qso.AwardGranted, rec, adifield.AWARD_GRANTED)
	writeCredit(qso.CreditSubmitted, rec, adifield.CREDIT_SUBMITTED)
	writeCredit(qso.CreditGranted, rec, adifield.CREDIT_GRANTED)
}

func writeAwards(awards []string, rec adif.Record, field adifield.Field) {
	writeString(rec, field, strings.Join(awards, ","))
}

func writeCredit(credit []*adifpb.Credit, rec adif.Record, field adifield.Field) {
	var elem = make([]string, 0)
	for _, c := range credit {
		e := c.Credit
		if c.QslMedium != "" {
			e += ":" + c.QslMedium
		}
		elem = append(elem, e)
	}
	writeString(rec, field, strings.Join(elem, ","))
}

func writeUploads(qso *adifpb.Qso, rec adif.Record) {
	if qso.Qrzcom != nil {
		writeString(rec, "qrzcom_qso_upload_status", writeUploadStatus(qso.Qrzcom.UploadStatus))
		writeDate(rec, "qrzcom_qso_upload_date", qso.Qrzcom.UploadDate.AsTime())
	}

	if qso.Hrdlog != nil {
		writeString(rec, "hrdlog_qso_upload_status", writeUploadStatus(qso.Hrdlog.UploadStatus))
		writeDate(rec, "hrdlog_qso_upload_date", qso.Hrdlog.UploadDate.AsTime())
	}

	if qso.Clublog != nil {
		writeString(rec, "clublog_qso_upload_status", writeUploadStatus(qso.Clublog.UploadStatus))
		writeDate(rec, "clublog_qso_upload_date", qso.Clublog.UploadDate.AsTime())
	}
}

func writeUploadStatus(status adifpb.UploadStatus) string {
	switch status {
	case adifpb.UploadStatus_UPLOAD_COMPLETE:
		return "Y"
	case adifpb.UploadStatus_DO_NOT_UPLOAD:
		return "N"
	case adifpb.UploadStatus_MODIFIED_AFTER_UPLOAD:
		return "M"
	default:
		return ""
	}
}

func writeQsls(qso *adifpb.Qso, rec adif.Record) {
	writeCardQsl(qso.Card, rec)
	writeQsl(qso.Eqsl, rec, adifield.EQSL_QSL_SENT, adifield.EQSL_QSLSDATE, adifield.EQSL_QSL_RCVD, adifield.EQSL_QSLRDATE)
	writeQsl(qso.Eqsl, rec, adifield.LOTW_QSL_SENT, adifield.LOTW_QSLSDATE, adifield.LOTW_QSL_RCVD, adifield.LOTW_QSLRDATE)
}

func writeCardQsl(qsl *adifpb.Qsl, rec adif.Record) {
	if qsl == nil {
		return
	}
	writeQsl(qsl, rec, adifield.QSL_SENT, adifield.QSLSDATE, adifield.QSL_RCVD, adifield.QSLRDATE)
	writeString(rec, adifield.QSL_SENT_VIA, qsl.SentVia)
	writeString(rec, adifield.QSL_RCVD_VIA, qsl.ReceivedVia)
	writeString(rec, adifield.QSLMSG, qsl.ReceivedMessage)
}

func writeQsl(qsl *adifpb.Qsl, rec adif.Record, sent, sentDate, rcvd, rcvdDate adifield.Field) {
	if qsl == nil {
		return
	}
	writeString(rec, sent, qsl.SentStatus)
	writeDate(rec, sentDate, qsl.SentDate.AsTime())
	writeString(rec, rcvd, qsl.ReceivedStatus)
	writeDate(rec, rcvdDate, qsl.ReceivedDate.AsTime())

}

func writeString(rec adif.Record, adifField adifield.Field, value string) {
	if value == "" {
		return
	}
	rec.Set(adifField, value)
}

func writeBool(rec adif.Record, adifField adifield.Field, value bool) {
	if !value {
		return
	}
	rec.Set(adifField, "Y")
}

func writeInt(rec adif.Record, adifField adifield.Field, value int64) {
	if value == 0 {
		return
	}
	rec.Set(adifField, strconv.FormatInt(value, 10))
}

func writeFloat(rec adif.Record, adifField adifield.Field, value float64, precision int) {
	if value == 0 {
		return
	}
	rec.Set(adifField, strconv.FormatFloat(value, 'g', precision, 64))
}

func writeLatLon(rec adif.Record, adifField adifield.Field, latLon float64, isLat bool) {
	if latLon == 0 {
		return
	}
	value := latLonToString(latLon, isLat)
	rec.Set(adifField, value)
}

func latLonToString(latLon float64, isLat bool) string {
	var cardinal string
	if isLat {
		if latLon >= 0 {
			cardinal = "N"
		} else {
			cardinal = "S"
		}
	} else {
		if latLon >= 0 {
			cardinal = "E"
		} else {
			cardinal = "W"
		}
	}
	degrees := math.Floor(math.Abs(latLon))
	minutes := (math.Abs(latLon) - math.Abs(degrees)) * 60
	return fmt.Sprintf("%s%03d %06.3f", cardinal, int(degrees), minutes)
}

func writeDate(rec adif.Record, adifField adifield.Field, value time.Time) {
	if value.Unix() == 0 {
		return
	}
	// YYYYMMDD
	rec.Set(adifField, dateToString(value))
}

func dateToString(value time.Time) string {
	return value.Format("20060102")
}

func writeTime(rec adif.Record, adifField adifield.Field, value time.Time) {
	if value.Unix() == 0 {
		return
	}
	// HHMMSS
	rec.Set(adifField, timeToString(value))
}

func timeToString(value time.Time) string {
	return value.Format("150405")
}
