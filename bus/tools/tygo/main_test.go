// SPDX-License-Identifier: MIT

package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestGeneratedDTOsMatchSource(t *testing.T) {
	raw, err := os.ReadFile("../../sdk/go/protocol/types.go")
	if err != nil {
		t.Fatal(err)
	}
	got, err := generate(raw)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("../../sdk/js/protocol.d.ts")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatal("generated DTOs are stale")
	}
	for _, declaration := range []string{"self_info?: SessionSelfInfo;", "export type SessionSelfInfo = DeliverySource;", "export interface WorkerHello extends HelloDescription", "timeout_ms?: number /* int64 */;", "data?: unknown;", "info: { [key: string]: unknown}"} {
		if !strings.Contains(string(got), declaration) {
			t.Errorf("missing wire mapping %q", declaration)
		}
	}
	if strings.Contains(string(got), "export const") || strings.Contains(string(got), ": any") {
		t.Fatal("generated DTOs contain runtime values or unsafe any")
	}
}

func TestUnknownExternalTypeFailsGeneration(t *testing.T) {
	_, err := generate([]byte("package protocol\ntype Added struct { Field external.Type `json:\"field\"` }"))
	if err == nil {
		t.Fatal("unmapped external type accepted")
	}
}

func TestTypeShapesComeFromGo(t *testing.T) {
	raw := []byte("package protocol\nconst PrivateConstant = 42\ntype Added struct { Value string `json:\"wire_name,omitempty\"`; Count *int64 `json:\"count,omitempty\"` }\ntype Alias = Added")
	got, err := generate(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"wire_name?: string;", "count?: number /* int64 */;", "export type Alias = Added;"} {
		if !strings.Contains(string(got), expected) {
			t.Errorf("missing %q", expected)
		}
	}
	if strings.Contains(string(got), "PrivateConstant") {
		t.Fatal("constant leaked into type-only API")
	}
}

// The method documentation refers to generated DTOs, and the public Go Caller
// supplies the request/result mapping. Catch e.g. TurnResult vs RunStatus drift.
func TestCallerJSDocMatchesGoSignatures(t *testing.T) {
	source, err := os.ReadFile("../../sdk/go/caller.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "caller.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	js, err := os.ReadFile("../../sdk/js/caller.js")
	if err != nil {
		t.Fatal(err)
	}
	docs := map[string]string{}
	for _, match := range regexp.MustCompile(`(?s)/\*\*(.*?)\*/\s+(\w+)\(`).FindAllStringSubmatch(string(js), -1) {
		docs[match[2]] = match[1]
	}
	for _, declaration := range file.Decls {
		method, ok := declaration.(*ast.FuncDecl)
		if !ok || method.Recv == nil || !method.Name.IsExported() || method.Name.Name == "WaitContext" {
			continue
		}
		name := strings.ToLower(method.Name.Name[:1]) + method.Name.Name[1:]
		doc, ok := docs[name]
		if !ok {
			t.Errorf("missing JS method documentation for Go Caller.%s", method.Name.Name)
			continue
		}
		result, ok := method.Type.Results.List[0].Type.(*ast.Ident)
		if !ok {
			t.Fatalf("review new Caller.%s result representation", method.Name.Name)
		}
		returns := "import(\"./protocol\")." + result.Name
		if result.Name == "error" {
			returns = "Record<string, never>"
		}
		if !strings.Contains(doc, "@returns {Promise<"+returns+">}") {
			t.Errorf("Caller.%s result documentation differs from Go %s", name, result.Name)
		}
		for _, field := range method.Type.Params.List {
			arg, ok := field.Type.(*ast.Ident)
			if !ok {
				continue
			} // context.Context is not a wire DTO.
			param := "import(\"./protocol\")." + arg.Name
			if arg.Name == "string" {
				param = "string"
			}
			if !strings.Contains(doc, "@param {"+param+"}") {
				t.Errorf("Caller.%s request documentation differs from Go %s", name, arg.Name)
			}
		}
	}
}
