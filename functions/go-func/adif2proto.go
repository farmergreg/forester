package forester

import (
	"errors"
	"io"
	"log"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/farmergreg/adif/v5"
	"github.com/farmergreg/spec/v6/adifield"
	adifpb "github.com/k0swe/adif-json-protobuf/go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func adifToProto(adifString string, createTime time.Time) (*adifpb.Adif, error) {
	adi := new(adifpb.Adif)
	created := timestamppb.New(createTime)
	adi.Header = &adifpb.Header{
		AdifVersion:      "3.1.1",
		CreatedTimestamp: created,
		ProgramId:        "forester-func",
		ProgramVersion:   "0.0.2",
	}
	reader := adif.NewADIDocumentReader(strings.NewReader(adifString), true)
	rec, _, err := reader.Next()
	for err == nil {
		adi.Qsos = append(adi.Qsos, recordToQso(rec))
		rec, _, err = reader.Next()
	}
	if !errors.Is(err, io.EOF) {
		return nil, err
	}
	return adi, nil
}

func recordToQso(record adif.Record) *adifpb.Qso {
	qso := new(adifpb.Qso)
	parseTopLevel(record, qso)
	parseAppDefined(record, qso)
	parseContactedStation(record, qso)
	parseLoggingStation(record, qso)
	parseContest(record, qso)
	parsePropagation(record, qso)
	parseAwardsAndCredit(record, qso)
	parseUploads(record, qso)
	parseQsls(record, qso)
	return qso
}

func parseTopLevel(record adif.Record, qso *adifpb.Qso) {
	qso.Band = record.Get(adifield.BAND)
	qso.BandRx = record.Get(adifield.BAND_RX)
	qso.Comment = record.Get(adifield.COMMENT)
	qso.DistanceKm = getUint32(record.Get(adifield.DISTANCE))
	qso.Freq = getFloat64(record.Get(adifield.FREQ))
	qso.FreqRx = getFloat64(record.Get(adifield.FREQ_RX))
	qso.Mode = record.Get(adifield.MODE)
	qso.Notes = record.Get(adifield.NOTES)
	qso.PublicKey = record.Get(adifield.PUBLIC_KEY)
	qso.Complete = record.Get(adifield.QSO_COMPLETE)
	qso.TimeOn = getTimestamp(record.Get(adifield.QSO_DATE), record.Get(adifield.TIME_ON))
	qso.TimeOff = getTimestamp(record.Get(adifield.QSO_DATE_OFF), record.Get(adifield.TIME_OFF))
	qso.Random = getBool(record.Get(adifield.QSO_RANDOM))
	qso.RstReceived = record.Get(adifield.RST_RCVD)
	qso.RstSent = record.Get(adifield.RST_SENT)
	qso.Submode = record.Get(adifield.SUBMODE)
	qso.Swl = getBool(record.Get(adifield.SWL))
}

func parseAppDefined(record adif.Record, qso *adifpb.Qso) {
	appDefined := map[string]string{}
	for field, value := range record.Fields() {
		if strings.HasPrefix(string(field), adifield.APP_) {
			appDefined[strings.ToLower(string(field))] = value // TODO converting to lower for compatibility. could this be left upper?
		}
	}
	if len(appDefined) > 0 {
		qso.AppDefined = appDefined
	}
}

func parseContactedStation(record adif.Record, qso *adifpb.Qso) {
	qso.ContactedStation = new(adifpb.Station)
	qso.ContactedStation.Address = record.Get(adifield.ADDRESS)
	qso.ContactedStation.Age = getUint32(record.Get(adifield.AGE))
	qso.ContactedStation.StationCall = record.Get(adifield.CALL)
	qso.ContactedStation.County = record.Get(adifield.CNTY)
	qso.ContactedStation.Continent = record.Get(adifield.CONT)
	qso.ContactedStation.OpCall = record.Get(adifield.CONTACTED_OP)
	qso.ContactedStation.Country = record.Get(adifield.COUNTRY)
	qso.ContactedStation.CqZone = getUint32(record.Get(adifield.CQZ))
	qso.ContactedStation.DarcDok = record.Get(adifield.DARC_DOK)
	qso.ContactedStation.Dxcc = getUint32(record.Get(adifield.DXCC))
	qso.ContactedStation.Email = record.Get(adifield.EMAIL)
	qso.ContactedStation.OwnerCall = record.Get(adifield.EQ_CALL)
	qso.ContactedStation.Fists = getUint32(record.Get(adifield.FISTS))
	qso.ContactedStation.FistsCc = getUint32(record.Get(adifield.FISTS_CC))
	qso.ContactedStation.GridSquare = record.Get(adifield.GRIDSQUARE)
	qso.ContactedStation.Iota = record.Get(adifield.IOTA)
	qso.ContactedStation.IotaIslandId = getUint32(record.Get(adifield.IOTA_ISLAND_ID))
	qso.ContactedStation.ItuZone = getUint32(record.Get(adifield.ITUZ))
	qso.ContactedStation.Latitude = getLatLon(record.Get(adifield.LAT))
	qso.ContactedStation.Longitude = getLatLon(record.Get(adifield.LON))
	qso.ContactedStation.OpName = record.Get(adifield.NAME)
	qso.ContactedStation.Pfx = record.Get(adifield.PFX)
	qso.ContactedStation.QslVia = record.Get(adifield.QSL_VIA)
	qso.ContactedStation.City = record.Get(adifield.QTH)
	qso.ContactedStation.Region = record.Get(adifield.REGION)
	qso.ContactedStation.Rig = record.Get(adifield.RIG)
	qso.ContactedStation.Power = getFloat64(record.Get(adifield.RX_PWR))
	qso.ContactedStation.Sig = record.Get(adifield.SIG)
	qso.ContactedStation.SigInfo = record.Get(adifield.SIG_INFO)
	qso.ContactedStation.SilentKey = getBool(record.Get(adifield.SILENT_KEY))
	qso.ContactedStation.Skcc = record.Get(adifield.SKCC)
	qso.ContactedStation.SotaRef = record.Get(adifield.SOTA_REF)
	qso.ContactedStation.State = record.Get(adifield.STATE)
	qso.ContactedStation.TenTen = getUint32(record.Get(adifield.TEN_TEN))
	qso.ContactedStation.Uksmg = getUint32(record.Get(adifield.UKSMG))
	qso.ContactedStation.UsacaCounties = record.Get(adifield.USACA_COUNTIES)
	qso.ContactedStation.VuccGrids = record.Get(adifield.VUCC_GRIDS)
	qso.ContactedStation.Web = record.Get(adifield.WEB)
}

func parseLoggingStation(record adif.Record, qso *adifpb.Qso) {
	qso.LoggingStation = new(adifpb.Station)
	qso.LoggingStation.AntennaAzimuth = getInt32(record.Get(adifield.ANT_AZ))
	qso.LoggingStation.AntennaElevation = getInt32(record.Get(adifield.ANT_EL))
	qso.LoggingStation.Antenna = record.Get(adifield.MY_ANTENNA)
	qso.LoggingStation.City = record.Get(adifield.MY_CITY)
	qso.LoggingStation.County = record.Get(adifield.MY_CNTY)
	qso.LoggingStation.Country = record.Get(adifield.MY_COUNTRY)
	qso.LoggingStation.CqZone = getUint32(record.Get(adifield.MY_CQ_ZONE))
	qso.LoggingStation.Dxcc = getUint32(record.Get(adifield.MY_DXCC))
	qso.LoggingStation.Fists = getUint32(record.Get(adifield.MY_FISTS))
	qso.LoggingStation.GridSquare = record.Get(adifield.MY_GRIDSQUARE)
	qso.LoggingStation.Iota = record.Get(adifield.MY_IOTA)
	qso.LoggingStation.IotaIslandId = getUint32(record.Get(adifield.MY_IOTA_ISLAND_ID))
	qso.LoggingStation.ItuZone = getUint32(record.Get(adifield.MY_ITU_ZONE))
	qso.LoggingStation.Latitude = getLatLon(record.Get(adifield.MY_LAT))
	qso.LoggingStation.Longitude = getLatLon(record.Get(adifield.MY_LON))
	qso.LoggingStation.OpName = record.Get(adifield.MY_NAME)
	qso.LoggingStation.PostalCode = record.Get(adifield.MY_POSTAL_CODE)
	qso.LoggingStation.Rig = record.Get(adifield.MY_RIG)
	qso.LoggingStation.Sig = record.Get(adifield.MY_SIG)
	qso.LoggingStation.SigInfo = record.Get(adifield.MY_SIG_INFO)
	qso.LoggingStation.SotaRef = record.Get(adifield.MY_SOTA_REF)
	qso.LoggingStation.State = record.Get(adifield.MY_STATE)
	qso.LoggingStation.Street = record.Get(adifield.MY_STREET)
	qso.LoggingStation.UsacaCounties = record.Get(adifield.MY_USACA_COUNTIES)
	qso.LoggingStation.VuccGrids = record.Get(adifield.MY_VUCC_GRIDS)
	qso.LoggingStation.OpCall = record.Get(adifield.OPERATOR)
	qso.LoggingStation.OwnerCall = record.Get(adifield.OWNER_CALLSIGN)
	qso.LoggingStation.StationCall = record.Get(adifield.STATION_CALLSIGN)
	qso.LoggingStation.Power = getFloat64(record.Get(adifield.TX_PWR))
}

func parseContest(record adif.Record, qso *adifpb.Qso) {
	contestID := record.Get(adifield.CONTEST_ID)
	if contestID != "" {
		qso.Contest = new(adifpb.ContestData)
		qso.Contest.ContestId = contestID
		qso.Contest.ArrlSection = record.Get(adifield.ARRL_SECT)
		qso.Contest.StationClass = record.Get(adifield.CLASS)
		qso.Contest.Check = record.Get(adifield.CHECK)
		qso.Contest.Precedence = record.Get(adifield.PRECEDENCE)
		qso.Contest.SerialReceived = record.Get(adifield.SRX)
		if qso.Contest.SerialReceived == "" {
			qso.Contest.SerialReceived = record.Get(adifield.SRX_STRING)
		}
		qso.Contest.SerialSent = record.Get(adifield.STX)
		if qso.Contest.SerialSent == "" {
			qso.Contest.SerialSent = record.Get(adifield.STX_STRING)
		}
	}
}

func parsePropagation(record adif.Record, qso *adifpb.Qso) {
	qso.Propagation = new(adifpb.Propagation)
	qso.Propagation.AIndex = getUint32(record.Get(adifield.A_INDEX))
	qso.Propagation.AntPath = record.Get(adifield.ANT_PATH)
	qso.Propagation.ForceInit = getBool(record.Get(adifield.FORCE_INIT))
	qso.Propagation.KIndex = getUint32(record.Get(adifield.K_INDEX))
	qso.Propagation.MaxBursts = getUint32(record.Get(adifield.MAX_BURSTS))
	qso.Propagation.MeteorShowerName = record.Get(adifield.MS_SHOWER)
	qso.Propagation.NrBursts = getUint32(record.Get(adifield.NR_BURSTS))
	qso.Propagation.NrPings = getUint32(record.Get(adifield.NR_PINGS))
	qso.Propagation.PropagationMode = record.Get(adifield.PROP_MODE)
	qso.Propagation.SatMode = record.Get(adifield.SAT_MODE)
	qso.Propagation.SatName = record.Get(adifield.SAT_NAME)
	qso.Propagation.SolarFluxIndex = getUint32(record.Get(adifield.SFI))
}

func parseAwardsAndCredit(record adif.Record, qso *adifpb.Qso) {
	qso.AwardSubmitted = parseAwards(record.Get(adifield.AWARD_SUBMITTED))
	qso.AwardGranted = parseAwards(record.Get(adifield.AWARD_GRANTED))
	qso.CreditSubmitted = parseCredit(record.Get(adifield.CREDIT_SUBMITTED))
	qso.CreditGranted = parseCredit(record.Get(adifield.CREDIT_GRANTED))
}

func parseAwards(awardString string) []string {
	if awardString == "" {
		return nil
	}
	return strings.Split(awardString, ",")
}

func parseCredit(creditString string) []*adifpb.Credit {
	if creditString == "" {
		return nil
	}
	credits := strings.Split(creditString, ",")
	ret := make([]*adifpb.Credit, len(credits))
	for i, c := range credits {
		cred := new(adifpb.Credit)
		cSplit := strings.Split(c, ":")
		cred.Credit = cSplit[0]
		if len(cSplit) > 1 {
			cred.QslMedium = cSplit[1]
		}
		ret[i] = cred
	}
	return ret
}

func parseUploads(record adif.Record, qso *adifpb.Qso) {
	qrzStatus := record.Get(adifield.QRZCOM_QSO_UPLOAD_STATUS)
	if qrzStatus != "" {
		qso.Qrzcom = new(adifpb.Upload)
		qso.Qrzcom.UploadStatus = translateUploadStatus(qrzStatus)
		qso.Qrzcom.UploadDate = getDate(record.Get(adifield.QRZCOM_QSO_UPLOAD_DATE))
	}

	hrdStatus := record.Get(adifield.HRDLOG_QSO_UPLOAD_STATUS)
	if hrdStatus != "" {
		qso.Hrdlog = new(adifpb.Upload)
		qso.Hrdlog.UploadStatus = translateUploadStatus(hrdStatus)
		qso.Hrdlog.UploadDate = getDate(record.Get(adifield.HRDLOG_QSO_UPLOAD_DATE))
	}

	clublogStatus := record.Get(adifield.CLUBLOG_QSO_UPLOAD_STATUS)
	if clublogStatus != "" {
		qso.Clublog = new(adifpb.Upload)
		qso.Clublog.UploadStatus = translateUploadStatus(clublogStatus)
		qso.Clublog.UploadDate = getDate(record.Get(adifield.CLUBLOG_QSO_UPLOAD_DATE))
	}
}

func translateUploadStatus(status string) adifpb.UploadStatus {
	switch status {
	case "Y":
		return adifpb.UploadStatus_UPLOAD_COMPLETE
	case "N":
		return adifpb.UploadStatus_DO_NOT_UPLOAD
	case "M":
		return adifpb.UploadStatus_MODIFIED_AFTER_UPLOAD
	default:
		return adifpb.UploadStatus_UNKNOWN
	}
}

func parseQsls(record adif.Record, qso *adifpb.Qso) {
	qso.Card = parseCardQsl(record)

	qso.Eqsl = parseQsl(record.Get(adifield.EQSL_QSL_SENT), record.Get(adifield.EQSL_QSL_RCVD), record.Get(adifield.EQSL_QSLRDATE), record.Get(adifield.EQSL_QSLSDATE))
	qso.Eqsl = parseQsl(record.Get(adifield.LOTW_QSL_SENT), record.Get(adifield.LOTW_QSL_RCVD), record.Get(adifield.LOTW_QSLRDATE), record.Get(adifield.LOTW_QSLSDATE))
}

func parseCardQsl(record adif.Record) *adifpb.Qsl {
	card := parseQsl(record.Get(adifield.QSL_SENT), record.Get(adifield.QSL_RCVD), record.Get(adifield.QSLRDATE), record.Get(adifield.QSLSDATE))
	if card == nil {
		return nil
	}
	card.SentVia = record.Get(adifield.QSL_SENT_VIA)
	card.ReceivedVia = record.Get(adifield.QSL_RCVD_VIA)
	card.ReceivedMessage = record.Get(adifield.QSLMSG)
	return card
}

func parseQsl(sent, received, receivedDate, sentDate string) *adifpb.Qsl {
	var noQsl = (sent == "" || sent == "N") &&
		(received == "" || received == "N")
	if noQsl {
		return nil
	}
	qsl := new(adifpb.Qsl)
	qsl.SentStatus = sent
	qsl.ReceivedDate = getDate(receivedDate)
	qsl.ReceivedStatus = received
	qsl.SentDate = getDate(sentDate)

	return qsl
}

func getLatLon(st string) float64 {
	if st == "" {
		return 0
	}
	r := regexp.MustCompile(`([NESW])(\d+) ([\d.]+)`)
	groups := r.FindStringSubmatch(st)
	cardinal := groups[1]
	degrees, _ := strconv.ParseFloat(groups[2], 64)
	minutes, _ := strconv.ParseFloat(groups[3], 64)
	retval := degrees + (minutes / 60.0)
	if cardinal == "S" || cardinal == "W" {
		retval *= -1
	}
	// 4 decimal places is enough; https://xkcd.com/2170/
	retval = math.Round(retval*10000) / 10000
	return retval
}

func getBool(st string) bool {
	return st == "Y"
}

func getFloat64(st string) float64 {
	fl, _ := strconv.ParseFloat(st, 64)
	return fl
}

func getUint32(s string) uint32 {
	i64, _ := strconv.ParseUint(s, 10, 32)
	return uint32(i64)
}

func getInt32(s string) int32 {
	i64, _ := strconv.ParseInt(s, 10, 32)
	return int32(i64)
}

func getTimestamp(dateStr string, timeStr string) *timestamppb.Timestamp {
	if dateStr == "" {
		return nil
	}
	if len(timeStr) == 4 {
		timeStr += "00"
	}
	t, err := time.Parse("20060102 150405", dateStr+" "+timeStr)
	if err != nil {
		log.Print(err)
	}
	return timestamppb.New(t)
}

func getDate(dateStr string) *timestamppb.Timestamp {
	if dateStr == "" {
		return nil
	}
	t, err := time.Parse("20060102", dateStr)
	if err != nil {
		log.Print(err)
	}
	return timestamppb.New(t)
}
