// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const maxPendingForwardedPerHost = 256

func decodeForward(raw []byte, source string) (Forward, string, error) {
	var value Forward
	if protocol.DecodeJSON(raw, &value) != nil || !validCaller(value.From, source) || (value.Request.Method == "message.send") != (value.Request.MessageID != "") {
		return Forward{}, "", errFrame
	}
	host, err := targetHost(value.Request)
	if err != nil || host == source {
		return Forward{}, "", errFrame
	}
	return value, host, nil
}

func validCaller(value Caller, host string) bool {
	return ownedPart(value.SessionID, host, false) && ownedPart(value.Name, host, true) && validToken(value.Product) &&
		strings.HasPrefix(value.PrivateGroup, "session:") && slices.Contains(value.Groups, value.PrivateGroup) && uniqueStrings(value.Groups)
}

func ownedPart(value, host string, spaces bool) bool {
	index := strings.LastIndexByte(value, '@')
	if index <= 0 || value[index+1:] != host {
		return false
	}
	part := value[:index]
	length := utf8.RuneCountInString(part)
	return length > 0 && length <= 128 && !strings.ContainsFunc(part, func(character rune) bool {
		return !unicode.IsPrint(character) || !spaces && unicode.IsSpace(character)
	})
}

func targetHost(request PublicRequest) (string, error) {
	params, err := protocol.DecodeParams(request.Method, request.Params)
	if err != nil {
		return "", errFrame
	}
	switch value := params.(type) {
	case *protocol.SessionListRequest:
		if value.SessionID != "" {
			return suffix(value.SessionID)
		}
		if !validHost(value.Host) {
			return "", errFrame
		}
		return value.Host, nil
	case *protocol.MessageSendRequest:
		if value.Group != "" {
			if !validHost(value.Host) {
				return "", errFrame
			}
			return value.Host, nil
		}
		labels := value.Targets
		if value.Target != "" {
			labels = []string{value.Target}
		}
		if len(labels) == 0 {
			return "", errFrame
		}
		host, err := suffix(labels[0])
		for _, label := range labels[1:] {
			if next, nextErr := suffix(label); nextErr != nil || next != host {
				return "", errFrame
			}
		}
		return host, err
	case *protocol.LaneDescribeRequest:
		if !validHost(value.Host) {
			return "", errFrame
		}
		return value.Host, nil
	case *protocol.LaneSpawnRequest:
		if value.ResumeSessionID != "" {
			return suffix(value.ResumeSessionID)
		}
		if !validHost(value.Host) {
			return "", errFrame
		}
		return value.Host, nil
	case *protocol.TurnRunRequest:
		return suffix(value.SessionID)
	case *protocol.SessionTarget:
		return suffix(value.SessionID)
	case *protocol.SessionCloseRequest:
		return suffix(value.SessionID)
	default:
		return "", errFrame
	}
}

func suffix(value string) (string, error) {
	index := strings.LastIndexByte(value, '@')
	if index <= 0 || !validHost(value[index+1:]) {
		return "", errFrame
	}
	return value[index+1:], nil
}

func validHost(value string) bool { return value != "local" && validToken(value) }

func validToken(value string) bool {
	if len(value) == 0 || len(value) > 32 || !((value[0] >= 'a' && value[0] <= 'z') || (value[0] >= '0' && value[0] <= '9')) {
		return false
	}
	for _, character := range value[1:] {
		if character != '-' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func uniqueStrings(values []string) bool {
	seen := map[string]bool{}
	for _, value := range values {
		if value == "" || seen[value] {
			return false
		}
		seen[value] = true
	}
	return true
}
