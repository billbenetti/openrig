import { it, expect, vi } from "vitest";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ConfigStore } from "../src/config-store.js";
import { SettingsStore } from "../../daemon/src/domain/user-settings/settings-store.js";
it("the tmux environment twins default to scoped and preserve file/env opt-in precedence", () => {
  const home=mkdtempSync(join(tmpdir(),"tmux-setting-")), file=join(home,"config.json");
  const cli=new ConfigStore(file), daemon=new SettingsStore(file);
  try {
    vi.stubEnv("OPENRIG_TERMINAL_INHERIT_TMUX_ENVIRONMENT",undefined);
    expect(cli.resolveWithSource("terminal.inherit_tmux_environment")).toMatchObject({value:false,source:"default"});
    expect(daemon.resolveOne("terminal.inherit_tmux_environment")).toMatchObject({value:false,source:"default"});
    writeFileSync(file, JSON.stringify({terminal:{inheritTmuxEnvironment:true}}));
    expect(cli.resolveWithSource("terminal.inherit_tmux_environment")).toMatchObject({value:true,source:"file"});
    expect(daemon.resolveOne("terminal.inherit_tmux_environment")).toMatchObject({value:true,source:"file"});
    vi.stubEnv("OPENRIG_TERMINAL_INHERIT_TMUX_ENVIRONMENT","false");
    expect(cli.resolveWithSource("terminal.inherit_tmux_environment")).toMatchObject({value:false,source:"env"});
    expect(daemon.resolveOne("terminal.inherit_tmux_environment")).toMatchObject({value:false,source:"env"});
  } finally { vi.unstubAllEnvs(); rmSync(home,{recursive:true,force:true}); }
});

it("the seat scope twins default to off and 2048, keep file/env precedence, and refuse a ceiling out of range", () => {
  const home=mkdtempSync(join(tmpdir(),"seat-scope-setting-")), file=join(home,"config.json");
  const cli=new ConfigStore(file), daemon=new SettingsStore(file);
  try {
    vi.stubEnv("OPENRIG_TERMINAL_SEAT_SCOPE",undefined); vi.stubEnv("OPENRIG_TERMINAL_SEAT_SCOPE_TASKS_MAX",undefined);
    for (const [key, value] of [["terminal.seat_scope", false], ["terminal.seat_scope_tasks_max", 2048]] as const) {
      expect(cli.resolveWithSource(key)).toMatchObject({value,source:"default"});
      expect(daemon.resolveOne(key)).toMatchObject({value,source:"default"});
    }
    writeFileSync(file, JSON.stringify({terminal:{seatScope:true,seatScopeTasksMax:512}}));
    expect(cli.resolveWithSource("terminal.seat_scope")).toMatchObject({value:true,source:"file"});
    expect(daemon.resolveOne("terminal.seat_scope")).toMatchObject({value:true,source:"file"});
    expect(cli.resolveWithSource("terminal.seat_scope_tasks_max")).toMatchObject({value:512,source:"file"});
    expect(daemon.resolveOne("terminal.seat_scope_tasks_max")).toMatchObject({value:512,source:"file"});
    vi.stubEnv("OPENRIG_TERMINAL_SEAT_SCOPE","false");
    expect(cli.resolveWithSource("terminal.seat_scope")).toMatchObject({value:false,source:"env"});
    expect(daemon.resolveOne("terminal.seat_scope")).toMatchObject({value:false,source:"env"});
    for (const bad of ["63", "4194305", "2048.5", "abc"]) expect(() => cli.set("terminal.seat_scope_tasks_max", bad)).toThrow(/\[64, 4194304\]|integer|number/);
    for (const bad of ["63", "4194305"]) expect(() => daemon.set("terminal.seat_scope_tasks_max", bad)).toThrow(/\[64, 4194304\]/);
    cli.set("terminal.seat_scope_tasks_max", "64");
    expect(daemon.resolveOne("terminal.seat_scope_tasks_max")).toMatchObject({value:64,source:"file"});
  } finally { vi.unstubAllEnvs(); rmSync(home,{recursive:true,force:true}); }
});
