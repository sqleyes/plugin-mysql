package plugin_mysql

import (
	"fmt"
	. "github.com/sqleyes/engine"
	"github.com/sqleyes/engine/abstract"
	"github.com/sqleyes/engine/util"
	"strings"
)

type Mysql struct {
	abstract.Plugin
	BPFFilter string
	Device    string
	packet    []byte
}

func (m *Mysql) React(msg any) (command abstract.Command) {
	switch v := msg.(type) {
	case abstract.Installed:
		plugin.Infof("%s", v.Text)
		command = abstract.Start
	case abstract.Broken:
		m.Handle(v)
	case abstract.ERROR:
		plugin.Errorf("%s \t", v.Text)
	}
	return
}

func (p *Mysql) Handle(broken abstract.Broken) {
	if len(p.packet) != 0 {
		broken.Payload = append(p.packet, broken.Payload...)
		p.packet = p.packet[:0]
	}
	buffer := util.NewByteBuffer(broken.Payload, false)
	for buffer.HasNext() {
		length, _ := p.resolveOne(buffer)
		if buffer.Len() < buffer.Position()+int64(length) {
			return
		}
		onePayload := buffer.Read(int64(length))
		oneBuffer := util.NewByteBuffer(onePayload, false)

		if strings.Index(p.BPFFilter, fmt.Sprintf("%d", broken.SrcPort)) != -1 {
			p.resolveServer(oneBuffer)
		} else {
			p.resolveClient(oneBuffer)
		}
	}
}

type Sql struct {
	Type string
	Text string
}

func (p *Mysql) resolveClient(buffer *util.ByteBuffer) {
	r := Sql{}
	switch buffer.ReadShort() {
	case COM_INIT_DB:
		r.Type = "COM_INIT_DB"
		r.Text = fmt.Sprintf("USE %s;", buffer.ReadEnd())
	case COM_DROP_DB:
		r.Type = "COM_DROP_DB"
		r.Text = fmt.Sprintf("Drop DB %s;", buffer.ReadEnd())
	case COM_CREATE_DB:
		r.Type = "COM_CREATE_DB"
		r.Text = fmt.Sprintf("%s", buffer.ReadEnd())
	case COM_QUERY:
		r.Type = "COM_QUERY"
		r.Text = fmt.Sprintf("%s", buffer.ReadEnd())
	case COM_STMT_PREPARE:
		r.Type = "COM_STMT_PREPARE"
		r.Text = fmt.Sprintf("%s", buffer.ReadEnd())
	case COM_STMT_SEND_LONG_DATA:
		r.Type = "COM_STMT_SEND_LONG_DATA"
		r.Text = fmt.Sprintf("%s", buffer.ReadEnd())
	case COM_STMT_RESET:
		r.Type = "COM_STMT_RESET"
		r.Text = fmt.Sprintf("%s", buffer.ReadEnd())
	case COM_STMT_EXECUTE:
		r.Type = "COM_STMT_EXECUTE"
		r.Text = fmt.Sprintf("%s", buffer.ReadEnd())
	default:
		r.Type = "Unknown"
		r.Text = "Unknown Data"
	}
	plugin.Infof("Client -> %s", r.Text)
}
func (p *Mysql) resolveServer(buffer *util.ByteBuffer) {
	//if buffer.Len() == 1 {
	//	plugin.Infof("Number of fields: %d", buffer.ReadShort())
	//	return
	//}
	r := Sql{}
	switch buffer.ReadShort() {
	case 0xff:
		r.Type = "Error"
		r.Text = fmt.Sprintf("Error %d %s %s", buffer.GetInt16(), buffer.Read(5), buffer.ReadEnd())
	case 0x00:
		r.Type = "Affected Rows"
		r.Text = fmt.Sprintf("Affected Rows: %d", buffer.ReadShort())
	case 0xfe:
		//plugin.Infof("End Catalog ->%s", buffer.ReadEnd())
		return
	case 0x03, 0x04, 0x05, 0x06, 0x08, 0x0F, 0x0B, 0x0C:
		//数据获取
		//v1 := buffer.ReadShort()
		//v2 := buffer.ReadShort()
		//v3 := buffer.ReadShort()
		//if buffer.ReadShort() == 0x64 && buffer.ReadShort() == 0x65 && buffer.ReadShort() == 0x66 {
		//	plugin.Infof("Catalog ->%s", buffer.ReadEnd())
		//} else {
		//	buffer.Position(-2)
		//	plugin.Infof("DataRow ->%s", buffer.ReadEnd())
		//}
		return
	default:
		return
	}
	plugin.Infof("Server -> %s", r.Text)
}

func (p *Mysql) resolveOne(buffer *util.ByteBuffer) (int, int) {
	if buffer.Position()+4 > buffer.Len() {
		p.packet = buffer.ReadEnd()
		return 999999, 0
	}
	length := int(uint32(buffer.ReadShort()) | uint32(buffer.ReadShort())<<8 | uint32(buffer.ReadShort())<<16)
	seq := int(buffer.ReadShort())
	return length, seq
}

var m = Mysql{}

var plugin = InstallPlugin(&m)
