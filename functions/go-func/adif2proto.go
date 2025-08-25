package forester

import (
	"log"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hamradiolog-net/adif-spec/v6/adifield"
	"github.com/hamradiolog-net/adif/v4"
	adifpb "github.com/k0swe/adif-json-protobuf/go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func adifToProto(adifString string, createTime time.Time) (*adifpb.Adif, error) {
	document := adif.NewDocument()
	_, err := document.ReadFrom(strings.NewReader(adifString))
	if err != nil {
		return nil, err
	}

	adi := new(adifpb.Adif)
	created := timestamppb.New(createTime)
	adi.Header = &adifpb.Header{
		AdifVersion:      "3.1.1",
		CreatedTimestamp: created,
		ProgramId:        "forester-func",
		ProgramVersion:   "0.0.2",
	}
	qsos := make([]*adifpb.Qso, len(document.Records))
	for i, rec := range document.Records {
		qsos[i] = recordToQso(rec)
	}
	adi.Qsos = qsos
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
	qso.Band = record[adifield.BAND]
	qso.BandRx = record[adifield.BAND_RX]
	qso.Comment = record[adifield.COMMENT]
	qso.DistanceKm = getUint32(record[adifield.DISTANCE])
	qso.Freq = getFloat64(record[adifield.FREQ])
	qso.FreqRx = getFloat64(record[adifield.FREQ_RX])
	qso.Mode = record[adifield.MODE]
	qso.Notes = record[adifield.NOTES]
	qso.PublicKey = record[adifield.PUBLIC_KEY]
	qso.Complete = record[adifield.QSO_COMPLETE]
	qso.TimeOn = getTimestamp(record[adifield.QSO_DATE], record[adifield.TIME_ON])
	qso.TimeOff = getTimestamp(record[adifield.QSO_DATE_OFF], record[adifield.TIME_OFF])
	qso.Random = getBool(record[adifield.QSO_RANDOM])
	qso.RstReceived = record[adifield.RST_RCVD]
	qso.RstSent = record[adifield.RST_SENT]
	qso.Submode = record[adifield.SUBMODE]
	qso.Swl = getBool(record[adifield.SWL])
}

func parseAppDefined(record adif.Record, qso *adifpb.Qso) {
	appDefined := map[string]string{}
	for i, field := range record {
		if strings.HasPrefix(string(i), adifield.APP_) {
			appDefined[strings.ToLower(string(i))] = field // TODO converting to lower for compatibility. could this be left upper?
		}
	}
	if len(appDefined) > 0 {
		qso.AppDefined = appDefined
	}
}

func parseContactedStation(record adif.Record, qso *adifpb.Qso) {
	qso.ContactedStation = new(adifpb.Station)
	qso.ContactedStation.Address = record[adifield.ADDRESS]
	qso.ContactedStation.Age = getUint32(record[adifield.AGE])
	qso.ContactedStation.StationCall = record[adifield.CALL]
	qso.ContactedStation.County = record[adifield.CNTY]
	qso.ContactedStation.Continent = record[adifield.CONT]
	qso.ContactedStation.OpCall = record[adifield.CONTACTED_OP]
	qso.ContactedStation.Country = record[adifield.COUNTRY]
	qso.ContactedStation.CqZone = getUint32(record[adifield.CQZ])
	qso.ContactedStation.DarcDok = record[adifield.DARC_DOK]
	qso.ContactedStation.Dxcc = getUint32(record[adifield.DXCC])
	qso.ContactedStation.Email = record[adifield.EMAIL]
	qso.ContactedStation.OwnerCall = record[adifield.EQ_CALL]
	qso.ContactedStation.Fists = getUint32(record[adifield.FISTS])
	qso.ContactedStation.FistsCc = getUint32(record[adifield.FISTS_CC])
	qso.ContactedStation.GridSquare = record[adifield.GRIDSQUARE]
	qso.ContactedStation.Iota = record[adifield.IOTA]
	qso.ContactedStation.IotaIslandId = getUint32(record[adifield.IOTA_ISLAND_ID])
	qso.ContactedStation.ItuZone = getUint32(record[adifield.ITUZ])
	qso.ContactedStation.Latitude = getLatLon(record[adifield.LAT])
	qso.ContactedStation.Longitude = getLatLon(record[adifield.LON])
	qso.ContactedStation.OpName = record[adifield.NAME]
	qso.ContactedStation.Pfx = record[adifield.PFX]
	qso.ContactedStation.QslVia = record[adifield.QSL_VIA]
	qso.ContactedStation.City = record[adifield.QTH]
	qso.ContactedStation.Region = record[adifield.REGION]
	qso.ContactedStation.Rig = record[adifield.RIG]
	qso.ContactedStation.Power = getFloat64(record[adifield.RX_PWR])
	qso.ContactedStation.Sig = record[adifield.SIG]
	qso.ContactedStation.SigInfo = record[adifield.SIG_INFO]
	qso.ContactedStation.SilentKey = getBool(record[adifield.SILENT_KEY])
	qso.ContactedStation.Skcc = record[adifield.SKCC]
	qso.ContactedStation.SotaRef = record[adifield.SOTA_REF]
	qso.ContactedStation.State = record[adifield.STATE]
	qso.ContactedStation.TenTen = getUint32(record[adifield.TEN_TEN])
	qso.ContactedStation.Uksmg = getUint32(record[adifield.UKSMG])
	qso.ContactedStation.UsacaCounties = record[adifield.USACA_COUNTIES]
	qso.ContactedStation.VuccGrids = record[adifield.VUCC_GRIDS]
	qso.ContactedStation.Web = record[adifield.WEB]
}

func parseLoggingStation(record adif.Record, qso *adifpb.Qso) {
	qso.LoggingStation = new(adifpb.Station)
	qso.LoggingStation.AntennaAzimuth = getInt32(record[adifield.ANT_AZ])
	qso.LoggingStation.AntennaElevation = getInt32(record[adifield.ANT_EL])
	qso.LoggingStation.Antenna = record[adifield.MY_ANTENNA]
	qso.LoggingStation.City = record[adifield.MY_CITY]
	qso.LoggingStation.County = record[adifield.MY_CNTY]
	qso.LoggingStation.Country = record[adifield.MY_COUNTRY]
	qso.LoggingStation.CqZone = getUint32(record[adifield.MY_CQ_ZONE])
	qso.LoggingStation.Dxcc = getUint32(record[adifield.MY_DXCC])
	qso.LoggingStation.Fists = getUint32(record[adifield.MY_FISTS])
	qso.LoggingStation.GridSquare = record[adifield.MY_GRIDSQUARE]
	qso.LoggingStation.Iota = record[adifield.MY_IOTA]
	qso.LoggingStation.IotaIslandId = getUint32(record[adifield.MY_IOTA_ISLAND_ID])
	qso.LoggingStation.ItuZone = getUint32(record[adifield.MY_ITU_ZONE])
	qso.LoggingStation.Latitude = getLatLon(record[adifield.MY_LAT])
	qso.LoggingStation.Longitude = getLatLon(record[adifield.MY_LON])
	qso.LoggingStation.OpName = record[adifield.MY_NAME]
	qso.LoggingStation.PostalCode = record[adifield.MY_POSTAL_CODE]
	qso.LoggingStation.Rig = record[adifield.MY_RIG]
	qso.LoggingStation.Sig = record[adifield.MY_SIG]
	qso.LoggingStation.SigInfo = record[adifield.MY_SIG_INFO]
	qso.LoggingStation.SotaRef = record[adifield.MY_SOTA_REF]
	qso.LoggingStation.State = record[adifield.MY_STATE]
	qso.LoggingStation.Street = record[adifield.MY_STREET]
	qso.LoggingStation.UsacaCounties = record[adifield.MY_USACA_COUNTIES]
	qso.LoggingStation.VuccGrids = record[adifield.MY_VUCC_GRIDS]
	qso.LoggingStation.OpCall = record[adifield.OPERATOR]
	qso.LoggingStation.OwnerCall = record[adifield.OWNER_CALLSIGN]
	qso.LoggingStation.StationCall = record[adifield.STATION_CALLSIGN]
	qso.LoggingStation.Power = getFloat64(record[adifield.TX_PWR])
}

func parseContest(record adif.Record, qso *adifpb.Qso) {
	contestID := record[adifield.CONTEST_ID]
	if contestID != "" {
		qso.Contest = new(adifpb.ContestData)
		qso.Contest.ContestId = contestID
		qso.Contest.ArrlSection = record[adifield.ARRL_SECT]
		qso.Contest.StationClass = record[adifield.CLASS]
		qso.Contest.Check = record[adifield.CHECK]
		qso.Contest.Precedence = record[adifield.PRECEDENCE]
		qso.Contest.SerialReceived = record[adifield.SRX]
		if qso.Contest.SerialReceived == "" {
			qso.Contest.SerialReceived = record[adifield.SRX_STRING]
		}
		qso.Contest.SerialSent = record[adifield.STX]
		if qso.Contest.SerialSent == "" {
			qso.Contest.SerialSent = record[adifield.STX_STRING]
		}
	}
}

func parsePropagation(record adif.Record, qso *adifpb.Qso) {
	qso.Propagation = new(adifpb.Propagation)
	qso.Propagation.AIndex = getUint32(record[adifield.A_INDEX])
	qso.Propagation.AntPath = record[adifield.ANT_PATH]
	qso.Propagation.ForceInit = getBool(record[adifield.FORCE_INIT])
	qso.Propagation.KIndex = getUint32(record[adifield.K_INDEX])
	qso.Propagation.MaxBursts = getUint32(record[adifield.MAX_BURSTS])
	qso.Propagation.MeteorShowerName = record[adifield.MS_SHOWER]
	qso.Propagation.NrBursts = getUint32(record[adifield.NR_BURSTS])
	qso.Propagation.NrPings = getUint32(record[adifield.NR_PINGS])
	qso.Propagation.PropagationMode = record[adifield.PROP_MODE]
	qso.Propagation.SatMode = record[adifield.SAT_MODE]
	qso.Propagation.SatName = record[adifield.SAT_NAME]
	qso.Propagation.SolarFluxIndex = getUint32(record[adifield.SFI])
}

func parseAwardsAndCredit(record adif.Record, qso *adifpb.Qso) {
	qso.AwardSubmitted = parseAwards(record[adifield.AWARD_SUBMITTED])
	qso.AwardGranted = parseAwards(record[adifield.AWARD_GRANTED])
	qso.CreditSubmitted = parseCredit(record[adifield.CREDIT_SUBMITTED])
	qso.CreditGranted = parseCredit(record[adifield.CREDIT_GRANTED])
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
	qrzStatus := record[adifield.QRZCOM_QSO_UPLOAD_STATUS]
	if qrzStatus != "" {
		qso.Qrzcom = new(adifpb.Upload)
		qso.Qrzcom.UploadStatus = translateUploadStatus(qrzStatus)
		qso.Qrzcom.UploadDate = getDate(record[adifield.QRZCOM_QSO_UPLOAD_DATE])
	}

	hrdStatus := record[adifield.HRDLOG_QSO_UPLOAD_STATUS]
	if hrdStatus != "" {
		qso.Hrdlog = new(adifpb.Upload)
		qso.Hrdlog.UploadStatus = translateUploadStatus(hrdStatus)
		qso.Hrdlog.UploadDate = getDate(record[adifield.HRDLOG_QSO_UPLOAD_DATE])
	}

	clublogStatus := record[adifield.CLUBLOG_QSO_UPLOAD_STATUS]
	if clublogStatus != "" {
		qso.Clublog = new(adifpb.Upload)
		qso.Clublog.UploadStatus = translateUploadStatus(clublogStatus)
		qso.Clublog.UploadDate = getDate(record[adifield.CLUBLOG_QSO_UPLOAD_DATE])
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

	qso.Eqsl = parseQsl(record[adifield.EQSL_QSL_SENT], record[adifield.EQSL_QSL_RCVD], record[adifield.EQSL_QSLRDATE], record[adifield.EQSL_QSLSDATE])
	qso.Eqsl = parseQsl(record[adifield.LOTW_QSL_SENT], record[adifield.LOTW_QSL_RCVD], record[adifield.LOTW_QSLRDATE], record[adifield.LOTW_QSLSDATE])
}

func parseCardQsl(record adif.Record) *adifpb.Qsl {
	card := parseQsl(record[adifield.QSL_SENT], record[adifield.QSL_RCVD], record[adifield.QSLRDATE], record[adifield.QSLSDATE])
	if card == nil {
		return nil
	}
	card.SentVia = record[adifield.QSL_SENT_VIA]
	card.ReceivedVia = record[adifield.QSL_RCVD_VIA]
	card.ReceivedMessage = record[adifield.QSLMSG]
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
