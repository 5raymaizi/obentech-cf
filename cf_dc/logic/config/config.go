package config

import (
	"cf_arbitrage/logic/rate"
	"cf_arbitrage/util/logger"
)

const (
	Normal int = iota
	PreTaker
)

type (
	Config struct {
		Queue rate.QueueConfig `json:"queue" mapstructure:"queue"`
		Log   logger.Config    `json:"log" mapstructure:"log"` //配置结构不好，Log配置应与Config同一级别。以后在考虑改动
		//开仓参数
		VolumeOpenMin float64 `json:"volume_open_min" mapstructure:"volume_open_min" yaml:"volume_open_min"`
		// tt\mt\tm是币
		VolumeOpenLimit   float64 `json:"volume_open_limit" mapstructure:"volume_open_limit" yaml:"volume_open_limit"`
		MTVolumeOpenMin   float64 `json:"mt_volume_open_min" mapstructure:"mt_volume_open_min" yaml:"mt_volume_open_min"`
		MTVolumeOpenLimit float64 `json:"mt_volume_open_limit" mapstructure:"mt_volume_open_limit" yaml:"mt_volume_open_limit"`
		TMVolumeOpenMin   float64 `json:"tm_volume_open_min" mapstructure:"tm_volume_open_min" yaml:"tm_volume_open_min"`
		TMVolumeOpenLimit float64 `json:"tm_volume_open_limit" mapstructure:"tm_volume_open_limit" yaml:"tm_volume_open_limit"`

		OpenTickNum        int `json:"open_tick_num" mapstructure:"open_tick_num" yaml:"open_tick_num"`
		ReplaceOpenTickNum int `json:"replace_open_tick_num" mapstructure:"replace_open_tick_num" yaml:"replace_open_tick_num"`

		// 平仓参数
		VolumeCloseMin     float64 `json:"volume_close_min" mapstructure:"volume_close_min" yaml:"volume_close_min"`
		VolumeCloseLimit   float64 `json:"volume_close_limit" mapstructure:"volume_close_limit" yaml:"volume_close_limit"`
		MTVolumeCloseMin   float64 `json:"mt_volume_close_min" mapstructure:"mt_volume_close_min" yaml:"mt_volume_close_min"`
		MTVolumeCloseLimit float64 `json:"mt_volume_close_limit" mapstructure:"mt_volume_close_limit" yaml:"mt_volume_close_limit"`
		TMVolumeCloseMin   float64 `json:"tm_volume_close_min" mapstructure:"tm_volume_close_min" yaml:"tm_volume_close_min"`
		TMVolumeCloseLimit float64 `json:"tm_volume_close_limit" mapstructure:"tm_volume_close_limit" yaml:"tm_volume_close_limit"`

		CloseTickNum        int `json:"close_tick_num" mapstructure:"close_tick_num" yaml:"close_tick_num"`
		ReplaceCloseTickNum int `json:"replace_close_tick_num" mapstructure:"replace_close_tick_num" yaml:"replace_close_tick_num"`

		MakerType       int `json:"maker_type" mapstructure:"maker_type" yaml:"maker_type"` // 0表示采用原有的挂单方式，1、2表示采用新的挂单方式
		SpreadLimitNum1 int `json:"spread_limit_num1" mapstructure:"spread_limit_num1" yaml:"spread_limit_num1"`
		SpreadLimitNum2 int `json:"spread_limit_num2" mapstructure:"spread_limit_num2" yaml:"spread_limit_num2"`
		// 杠杆倍数,改动需重启程序
		Level1 int `json:"level1" mapstructure:"level1" yaml:"level1"`
		Level2 int `json:"level2" mapstructure:"level2" yaml:"level2"`

		TotalLimit       float64 `json:"total_limit" mapstructure:"total_limit" yaml:"total_limit"`
		LimitPosLong     float64 `json:"limit_pos_long" mapstructure:"limit_pos_long" yaml:"limit_pos_long"`
		LimitPosShort    float64 `json:"limit_pos_short" mapstructure:"limit_pos_short" yaml:"limit_pos_short"`
		VolumeThreshold0 float64 `json:"volume_threshold0" mapstructure:"volume_threshold0" yaml:"volume_threshold0"`
		VolumeThreshold1 float64 `json:"volume_threshold1" mapstructure:"volume_threshold1" yaml:"volume_threshold1"`
		VolumeThreshold2 float64 `json:"volume_threshold2" mapstructure:"volume_threshold2" yaml:"volume_threshold2"`
		OpenAllowed      bool    `json:"open_allowed" mapstructure:"open_allowed" yaml:"open_allowed"`
		CloseAllowed     bool    `json:"close_allowed" mapstructure:"close_allowed" yaml:"close_allowed"`
		OpenNow          bool    `json:"open_now" mapstructure:"open_now" yaml:"open_now"`
		CloseNow         bool    `json:"close_now" mapstructure:"close_now" yaml:"close_now"`
		// Mode                 rate.RunMode `json:"mode" mapstructure:"mode" yaml:"mode"` // 默认空 双边taker, TM(spot taker, swap maker), MT(spot maker, swap taker)
		SwitchSwapThreshold  float64 `json:"switch_swap_threshold" mapstructure:"switch_swap_threshold" yaml:"switch_swap_threshold"`
		ExceptionExThreshold float64 `json:"exception_ex_threshold" mapstructure:"exception_ex_threshold" yaml:"exception_ex_threshold"`
		TotalExThreshold     float64 `json:"total_ex_threshold" mapstructure:"total_ex_threshold" yaml:"total_ex_threshold"`
		FundingLimit         float64 `json:"funding_limit" mapstructure:"funding_limit" yaml:"funding_limit"`

		Factor factor `json:"factor" mapstructure:"factor" yaml:"factor"`

		UseMMApi bool `json:"use_mm_api" mapstructure:"use_mm_api" yaml:"use_mm_api"` // 币安用，api使用mm通道

		PTMode                       int     `json:"pre_taker_mode" mapstructure:"pre_taker_mode" yaml:"pre_taker_mode"`
		PreTakerVolumeThresholdOpen  float64 `json:"pt_volume_threshold_open" mapstructure:"pt_volume_threshold_open" yaml:"pt_volume_threshold_open"`
		PreTakerVolumeOpenLimit      float64 `json:"pt_volume_open_limit" mapstructure:"pt_volume_open_limit" yaml:"pt_volume_open_limit"`
		PreTakerVolumeThresholdClose float64 `json:"pt_volume_threshold_close" mapstructure:"pt_volume_threshold_close" yaml:"pt_volume_threshold_close"`
		PreTakerVolumeCloseLimit     float64 `json:"pt_volume_close_limit" mapstructure:"pt_volume_close_limit" yaml:"pt_volume_close_limit"`
		PreTakerOpenTickNum          int     `json:"pt_open_tick_num" mapstructure:"pt_open_tick_num" yaml:"pt_open_tick_num"`
		PreTakerReplaceOpenTickNum   int     `json:"pt_replace_open_tick_num" mapstructure:"pt_replace_open_tick_num" yaml:"pt_replace_open_tick_num"`
		PreTakerCloseTickNum         int     `json:"pt_close_tick_num" mapstructure:"pt_close_tick_num" yaml:"pt_close_tick_num"`
		PreTakerReplaceCloseTickNum  int     `json:"pt_replace_close_tick_num" mapstructure:"pt_replace_close_tick_num" yaml:"pt_replace_close_tick_num"`
		StopLossSrDelta              float64 `json:"stop_loss_sr_delta" mapstructure:"stop_loss_sr_delta" yaml:"stop_loss_sr_delta"`

		// maker-maker 挂单 tn是tick num,rtn是replace tick num,减少序列化开销
		MmSwap1OpenTickNum  int `json:"mm_1_open_tn" mapstructure:"mm_1_open_tn" yaml:"mm_1_open_tn"`
		MmSwap2OpenTickNum  int `json:"mm_2_open_tn" mapstructure:"mm_2_open_tn" yaml:"mm_2_open_tn"`
		MmSwap1CloseTickNum int `json:"mm_1_close_tn" mapstructure:"mm_1_close_tn" yaml:"mm_1_close_tn"`
		MmSwap2CloseTickNum int `json:"mm_2_close_tn" mapstructure:"mm_2_close_tn" yaml:"mm_2_close_tn"`

		MmSwap1OpenReplaceTickNum  int `json:"mm_1_open_rtn" mapstructure:"mm_1_open_rtn" yaml:"mm_1_open_rtn"`
		MmSwap2OpenReplaceTickNum  int `json:"mm_2_open_rtn" mapstructure:"mm_2_open_rtn" yaml:"mm_2_open_rtn"`
		MmSwap1CloseReplaceTickNum int `json:"mm_1_close_rtn" mapstructure:"mm_1_close_rtn" yaml:"mm_1_close_rtn"`
		MmSwap2CloseReplaceTickNum int `json:"mm_2_close_rtn" mapstructure:"mm_2_close_rtn" yaml:"mm_2_close_rtn"`

		// mhtn是maker hedge tick num,mhrtn是maker hedge replace tick num,减少序列化开销
		MmSwap1OpenMakerHedgeTickNum  int `json:"mm_1_open_mhtn" mapstructure:"mm_1_open_mhtn" yaml:"mm_1_open_mhtn"`
		MmSwap2OpenMakerHedgeTickNum  int `json:"mm_2_open_mhtn" mapstructure:"mm_2_open_mhtn" yaml:"mm_2_open_mhtn"`
		MmSwap1CloseMakerHedgeTickNum int `json:"mm_1_close_mhtn" mapstructure:"mm_1_close_mhtn" yaml:"mm_1_close_mhtn"`
		MmSwap2CloseMakerHedgeTickNum int `json:"mm_2_close_mhtn" mapstructure:"mm_2_close_mhtn" yaml:"mm_2_close_mhtn"`

		MmSwap1OpenMakerHedgeReplaceTickNum  int `json:"mm_1_open_mhrtn" mapstructure:"mm_1_open_mhrtn" yaml:"mm_1_open_mhrtn"`
		MmSwap2OpenMakerHedgeReplaceTickNum  int `json:"mm_2_open_mhrtn" mapstructure:"mm_2_open_mhrtn" yaml:"mm_2_open_mhrtn"`
		MmSwap1CloseMakerHedgeReplaceTickNum int `json:"mm_1_close_mhrtn" mapstructure:"mm_1_close_mhrtn" yaml:"mm_1_close_mhrtn"`
		MmSwap2CloseMakerHedgeReplaceTickNum int `json:"mm_2_close_mhrtn" mapstructure:"mm_2_close_mhrtn" yaml:"mm_2_close_mhrtn"`

		// mh 是maker hedge, th 是taker hedge
		MmOpenMakerHedgeThreshold  float64 `json:"mm_open_mh_threshold" mapstructure:"mm_open_mh_threshold" yaml:"mm_open_mh_threshold"`
		MmOpenTakerHedgeThreshold  float64 `json:"mm_open_th_threshold" mapstructure:"mm_open_th_threshold" yaml:"mm_open_th_threshold"`
		MmCloseMakerHedgeThreshold float64 `json:"mm_close_mh_threshold" mapstructure:"mm_close_mh_threshold" yaml:"mm_close_mh_threshold"`
		MmCloseTakerHedgeThreshold float64 `json:"mm_close_th_threshold" mapstructure:"mm_close_th_threshold" yaml:"mm_close_th_threshold"`

		MmOpenVolume  float64 `json:"mm_open_volume" mapstructure:"mm_open_volume" yaml:"mm_open_volume"`
		MmCloseVolume float64 `json:"mm_close_volume" mapstructure:"mm_close_volume" yaml:"mm_close_volume"`

		// mm模式下，选择挂单方向，0表示两边都挂，1表示只挂swap1，2表示只挂swap2
		MMSwitchSide int `json:"mm_switch_side" mapstructure:"mm_switch_side" yaml:"mm_switch_side"`

		LongFundingLimit  float64 `json:"long_funding_limit" mapstructure:"long_funding_limit" yaml:"long_funding_limit"`
		ShortFundingLimit float64 `json:"short_funding_limit" mapstructure:"short_funding_limit" yaml:"short_funding_limit"`

		SrMode int `json:"sr_mode" mapstructure:"sr_mode" yaml:"sr_mode"`

		// u相关告警分级, https://digifinex.sg.larksuite.com/wiki/Z5uHwC7I3iJmPtkyUjtllGfMggc
		WarnValueLimit float64 `json:"warn_value_limit" mapstructure:"warn_value_limit" yaml:"warn_value_limit"`
		P1Level        int     `json:"p1_level" mapstructure:"p1_level" yaml:"p1_level"`
		P3Level        int     `json:"p3_level" mapstructure:"p3_level" yaml:"p3_level"`

		// 是否开启统计撤单
		EnableStatCancel bool `json:"enable_stc" mapstructure:"enable_stc" yaml:"enable_stc"`

		// 多方式并发控制及开关
		// 开平函数多协程参数, 先设置默认最多3个协程
		TTEnabled bool `json:"tt_enabled" mapstructure:"tt_enabled" yaml:"tt_enabled"`
		MTEnabled bool `json:"mt_enabled" mapstructure:"mt_enabled" yaml:"mt_enabled"`
		TMEnabled bool `json:"tm_enabled" mapstructure:"tm_enabled" yaml:"tm_enabled"`
		MMEnabled bool `json:"mm_enabled" mapstructure:"mm_enabled" yaml:"mm_enabled"`
		// ConcurrencyLimit   int  `json:"concurrency_limit" mapstructure:"concurrency_limit" yaml:"concurrency_limit"`
		TTConcurrencyLimit int `json:"tt_concurrency_limit" mapstructure:"tt_concurrency_limit" yaml:"tt_concurrency_limit"`
		MTConcurrencyLimit int `json:"mt_concurrency_limit" mapstructure:"mt_concurrency_limit" yaml:"mt_concurrency_limit"`
		TMConcurrencyLimit int `json:"tm_concurrency_limit" mapstructure:"tm_concurrency_limit" yaml:"tm_concurrency_limit"`
		MMConcurrencyLimit int `json:"mm_concurrency_limit" mapstructure:"mm_concurrency_limit" yaml:"mm_concurrency_limit"`
	}

	factor struct {
		Enable         bool    `json:"enable" mapstructure:"enable" yaml:"enable"`
		ColoOkx        bool    `json:"colo_okx" mapstructure:"colo_okx" yaml:"colo_okx"`
		SubscribeTrade bool    `json:"subscribe_trade" mapstructure:"subscribe_trade" yaml:"subscribe_trade"`
		IMB1Threshold  float64 `json:"imb1_threshold" mapstructure:"imb1_threshold" yaml:"imb1_threshold"`
		IMB2Threshold  float64 `json:"imb2_threshold" mapstructure:"imb2_threshold" yaml:"imb2_threshold"`

		MMIMB1Up   float64 `json:"mm_imb1_up" mapstructure:"mm_imb1_up" yaml:"mm_imb1_up"`
		MMIMB2Up   float64 `json:"mm_imb2_up" mapstructure:"mm_imb2_up" yaml:"mm_imb2_up"`
		MMIMB1Down float64 `json:"mm_imb1_down" mapstructure:"mm_imb1_down" yaml:"mm_imb1_down"`
		MMIMB2Down float64 `json:"mm_imb2_down" mapstructure:"mm_imb2_down" yaml:"mm_imb2_down"`
	}
)

// 实现IMBFactorConfig接口
func (c *factor) IsEnabled() bool {
	return c.Enable
}

func (c *factor) GetIMB1Threshold() float64 {
	return c.IMB1Threshold
}

func (c *factor) GetIMB2Threshold() float64 {
	return c.IMB2Threshold
}

func (c *factor) GetMMIMB1UpThreshold() float64 {
	return c.MMIMB1Up
}

func (c *factor) GetMMIMB1DownThreshold() float64 {
	return c.MMIMB1Down
}

func (c *factor) GetMMIMB2UpThreshold() float64 {
	return c.MMIMB2Up
}

func (c *factor) GetMMIMB2DownThreshold() float64 {
	return c.MMIMB2Down
}
