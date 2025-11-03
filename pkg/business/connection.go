//=============================================================================
/*
Copyright © 2023 Andrea Carboni andrea.carboni71@gmail.com

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
//=============================================================================

package business

import (
	"github.com/tradalia/core/auth"
	"github.com/tradalia/core/datatype"
	"github.com/tradalia/core/msg"
	"github.com/tradalia/core/req"
	"github.com/tradalia/system-adapter/pkg/adapter"
	"sync"
)

//=============================================================================

var userConnections = struct {
	sync.RWMutex
	m map[string]*UserConnections
}{m: make(map[string]*UserConnections)}

//=============================================================================
//===
//=== Public methods
//===
//=============================================================================

func GetConnections(c *auth.Context, filter map[string]any, offset int, limit int) *[]*ConnectionInfo {
	userConnections.RLock()
	defer userConnections.RUnlock()

	us := c.Session
	uc,found := userConnections.m[us.Username]

	var list []*ConnectionInfo

	if found {
		for _, ctx := range uc.contexts {
			ci := ConnectionInfo{
				Username      : ctx.Username,
				ConnectionCode: ctx.ConnectionCode,
				SystemCode    : ctx.GetAdapterInfo().Code,
				SystemName    : ctx.GetAdapterInfo().Name,
			}
			list = append(list, &ci)
		}
	}

	return &list
}

//=============================================================================

func GetConnectionsToRefresh() []*adapter.ConnectionContext {
	userConnections.RLock()
	defer userConnections.RUnlock()

	var list []*adapter.ConnectionContext

	for _,uc := range userConnections.m {
		for _, ctx := range uc.contexts {
			if ctx.NeedsRefresh() {
				list = append(list, ctx)
			}
		}
	}

	return list
}

//=============================================================================

func GetConnectionContextByInstanceCode(instanceCode string) *adapter.ConnectionContext {
	userConnections.RLock()
	defer userConnections.RUnlock()

	//TODO
	//for _,uc := range userConnections.m {
	//	for _,ctx := range uc.contexts {
	//		if ctx.InstanceCode == instanceCode {
	//			return ctx
	//		}
	//	}
	//}

	return nil
}

//=============================================================================

func Connect(c *auth.Context, connectionCode string, cs *ConnectionSpec) (*ConnectionResult, error) {
	userConnections.Lock()
	defer userConnections.Unlock()

	user := c.Session.Username
	uc,found := userConnections.m[user]

	//--- Add entry if it is the first time

	if !found {
		uc = NewUserConnections()
		userConnections.m[user] = uc
	}

	ctx,found := uc.contexts[connectionCode]
	if found {
		if ctx.IsConnected() {
			return &ConnectionResult{
				Status : ConnectionStatusConnected,
				Message: "Already connected",
			}, nil
		}

		if ctx.IsConnecting() {
			return &ConnectionResult{
				Status : ConnectionStatusConnecting,
				Message: "Still connecting",
			}, nil
		}
	}

	ad,ok := adapters[cs.SystemCode]
	if !ok {
		return nil, req.NewNotFoundError("System not found: %v", cs.SystemCode)
	}

	var err error
	ctx,err = adapter.NewConnectionContext(c.Session.Username, connectionCode, c.Gin.Request.Host, ad, cs.ConfigParams, cs.ConnectParams)
	if err != nil {
		return &ConnectionResult{
			Status : ConnectionStatusError,
			Message: err.Error(),
		}, nil
	}

	//--- It is better to store again the context even if it is already there: the user could use the
	//--- same connection code but with a different adapter

	uc.contexts[connectionCode] = ctx

	res := &ConnectionResult{
		Status : ConnectionStatusError,
		Action: ConnectionActionNone,
	}

	cr,err := ctx.Connect()
	if err != nil {
		res.Message = err.Error()
		return res,nil
	}

	err = sendConnectionChangeMessage(c, ctx)
	if err != nil {
		return &ConnectionResult{
			Message: err.Error(),
		}, nil
	}

	switch cr {
		case adapter.ConnectionResultConnected:
			res.Status = ConnectionStatusConnected

		case adapter.ConnectionResultOpenUrl:
			res.Status  = ConnectionStatusConnecting
			res.Action  = ConnectionActionOpenUrl
			res.Message = ctx.GetAdapterAuthUrl()

		//TODO: to review: hardcoded url
		case adapter.ConnectionResultProxyUrl:
			res.Status  = ConnectionStatusConnecting
			res.Action  = ConnectionActionOpenUrl
			res.Message = "https://tradalia-server:8449/api/system/v1/weblogin/"+ user +"/"+ connectionCode +"/login"
	}

	return res, nil
}

//=============================================================================

func Disconnect(c *auth.Context, connectionCode string) error {
	user := c.Session.Username

	userConnections.Lock()
	defer userConnections.Unlock()

	uc, ok := userConnections.m[user]
	if !ok {
		return req.NewNotFoundError("Connection not found for user: %v", user)
	}

	ctx, found := uc.contexts[connectionCode]
	if !found {
		return req.NewNotFoundError("Connection not found: %v", connectionCode)
	}

	if ctx.IsDisconnected() {
		return nil
	}

	err := sendConnectionChangeMessage(c, ctx)
	if err != nil {
		return req.NewServerErrorByError(err)
	}

	delete(uc.contexts, connectionCode)
	_ = ctx.Disconnect()

	return nil
}

//=============================================================================
//===
//=== Services
//===
//=============================================================================

func GetRootSymbols(c *auth.Context, connectionCode string, filter string) ([]*adapter.RootSymbol, error){
	ctx,err := getConnectionContext(c, connectionCode)
	if err != nil {
		return nil,err
	}

	return ctx.GetRootSymbols(filter)
}

//=============================================================================

func GetRootSymbol(c *auth.Context, connectionCode string, root string) (*adapter.RootSymbol, error){
	ctx,err := getConnectionContext(c, connectionCode)
	if err != nil {
		return nil,err
	}

	return ctx.GetRootSymbol(root)
}

//=============================================================================

func GetInstruments(c *auth.Context, connectionCode string, root string) ([]*adapter.Instrument, error){
	ctx,err := getConnectionContext(c, connectionCode)
	if err != nil {
		return nil,err
	}

	return ctx.GetInstruments(root)
}

//=============================================================================

func GetPriceBars(c *auth.Context, connectionCode string, symbol string, date datatype.IntDate) (*adapter.PriceBars, error){
	ctx,err := getConnectionContext(c, connectionCode)
	if err != nil {
		return nil,err
	}

	return ctx.GetPriceBars(symbol, date)
}

//=============================================================================

func GetAccounts(c *auth.Context, connectionCode string) ([]*adapter.Account, error){
	ctx,err := getConnectionContext(c, connectionCode)
	if err != nil {
		return nil,err
	}

	return ctx.GetAccounts()
}

//=============================================================================

func GetOrders(c *auth.Context, connectionCode string) (any, error){
	return nil,nil
}

//=============================================================================

func GetPositions(c *auth.Context, connectionCode string) (any, error){
	return nil,nil
}

//=============================================================================

func TestAdapter(c *auth.Context, connectionCode string, tar *TestAdapterRequest) (string, error){
	userConnections.RLock()

	user := c.Session.Username
	uc, ok := userConnections.m[user]
	if !ok {
		userConnections.RUnlock()
		return "",req.NewNotFoundError("Connection not found for user: %v", user)
	}

	ctx, found := uc.contexts[connectionCode]
	userConnections.RUnlock()
	if !found {
		return "",req.NewNotFoundError("Connection not found: %v", connectionCode)
	}

	return ctx.TestAdapter(tar.Service, tar.Query)
}

//=============================================================================
//===
//=== Private methods
//===
//=============================================================================

func sendConnectionChangeMessage(c *auth.Context, ctx *adapter.ConnectionContext) error {
	ccm := ConnectionChangeSystemMessage{
		Username      : ctx.Username,
		ConnectionCode: ctx.ConnectionCode,
		SystemCode    : ctx.GetAdapterInfo().Code,
		Status        : ctx.GetStatus(),
	}
	err := msg.SendMessage(msg.ExSystem, msg.SourceConnection, msg.TypeChange, &ccm)

	if err != nil {
		c.Log.Error("sendConnectionChangeMessage: Could not publish the change message", "error", err.Error())
		return err
	}

	return nil
}

//=============================================================================

func getConnectionContext(c *auth.Context, connectionCode string) (*adapter.ConnectionContext, error) {
	userConnections.RLock()

	user := c.Session.OnBehalfOf
	uc, ok := userConnections.m[user]
	if !ok {
		userConnections.RUnlock()
		return nil,req.NewNotFoundError("Connection not found for user: %v", user)
	}

	ctx, found := uc.contexts[connectionCode]
	userConnections.RUnlock()
	if !found {
		return nil,req.NewNotFoundError("Connection not found: %v", connectionCode)
	}

	return ctx,nil
}

//=============================================================================
