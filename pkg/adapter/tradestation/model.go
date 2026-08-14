//=============================================================================
//===
//=== Copyright (C) 2025-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================

package tradestation

import (
	"net/http"

	"github.com/algotiqa/system-adapter/pkg/adapter"
)

//=============================================================================
//--- Config parameters

const (
	//--- Config params

	ParamAccount  = "account"
	ParamAuthType = "authType"

	//--- Connection params (authType="browser" )

	ParamClientId     = "clientId"
	ParamClientSecret = "clientSecret"
	ParamClientCode   = "clientCode"

	//--- List options

	ParamAccountTest = "test"
	ParamAccountLive = "live"

	ParamAuthTypeBrowser  = "browser"
	ParamAuthTypeInternal = "internal"
)

//=============================================================================

const (
	LiveAPI = "https://api.tradestation.com"
	DemoAPI = "https://sim-api.tradestation.com"

	AuthorizeUrl  = "https://signin.tradestation.com/authorize"
	OauthTokenUrl = "https://signin.tradestation.com/oauth/token"

	LoginPageUrl       = "https://my.tradestation.com/api/auth/login?returnTo=%2F"
	LoginPostUrl       = "https://signin.tradestation.com/usernamepassword/login"
	LoginCallbackUrl   = "https://signin.tradestation.com/login/callback"
	LoginTwoFAUrl      = "https://signin.tradestation.com/u/mfa-otp-challenge"
	LoginTwoFAPath     = "/u/mfa-otp-challenge"
	LoginDashboardPath = "/dashboard"
	RefreshTokenUrl    = "https://my.tradestation.com/api/auth/token"
)

//=============================================================================

var configParams = []*adapter.ParamDef{
	{
		Name:     ParamAccount,
		Type:     adapter.ParamTypeList,
		DefValue: ParamAccountTest,
		Nullable: false,
		Values:   []string{ParamAccountTest, ParamAccountLive},
	},
	{
		Name:     ParamAuthType,
		Type:     adapter.ParamTypeList,
		DefValue: ParamAuthTypeBrowser,
		Nullable: false,
		Values:   []string{ParamAuthTypeBrowser, ParamAuthTypeInternal},
	},
}

//-----------------------------------------------------------------------------

var connectParamsBrowser = []*adapter.ParamDef{
	{
		Name:     ParamClientId,
		Type:     adapter.ParamTypePassword,
		DefValue: "",
		Nullable: false,
		MinValue: 0,
		MaxValue: 64,
	},
	{
		Name:     ParamClientSecret,
		Type:     adapter.ParamTypePassword,
		DefValue: "",
		Nullable: false,
		MinValue: 0,
		MaxValue: 64,
	},
}

//-----------------------------------------------------------------------------

var connectParamsInternal = []*adapter.ParamDef{
	{
		Name:     adapter.ParamUsername,
		Type:     adapter.ParamTypeString,
		DefValue: "",
		Nullable: false,
		MinValue: 0,
		MaxValue: 64,
	},
	{
		Name:     adapter.ParamPassword,
		Type:     adapter.ParamTypePassword,
		DefValue: "",
		Nullable: false,
		MinValue: 0,
		MaxValue: 64,
	},
	{
		Name:     adapter.ParamTwoFACode,
		Type:     adapter.ParamTypeString,
		DefValue: "",
		Nullable: false,
		MinValue: 0,
		MaxValue: 64,
	},
}

//-----------------------------------------------------------------------------

var connectParamsCode = []*adapter.ParamDef{
	{
		Name:     ParamClientCode,
		Type:     adapter.ParamTypeString,
		DefValue: "",
		Nullable: false,
		MinValue: 0,
		MaxValue: 128,
	},
}

//-----------------------------------------------------------------------------

var info = adapter.Info{
	Code:                 "TS",
	Name:                 "Tradestation",
	ConfigParams:         configParams,
	SupportsData:         true,
	SupportsBroker:       true,
	SupportsMultipleData: false,
	SupportsInventory:    true,
}

//=============================================================================

type ConfigParams struct {
	Account  string
	AuthType string
}

//=============================================================================

type ConnectParams struct {
	Username     string
	Password     string
	TwoFACode    string
	ClientId     string
	ClientSecret string
	ClientCode   string
}

//=============================================================================

type tradestation struct {
	configParams  *ConfigParams
	connectParams *ConnectParams
	client        *http.Client
	header        *http.Header
	accessToken   string
	refreshToken  string
	clientId      string
	apiUrl        string
}

//=============================================================================
