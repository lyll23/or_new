# or_new

每天保存 OpenRouter 完整公开模型目录，按目录采集各模型与全站统计的原始响应；由 GitHub 定时执行、按月 Release 数据附件暂存，电脑在线后补下载到本地并进行可追溯增量提取。

## 范围与保存原则

- 每轮先保存 `output_modalities=all` 的完整公开模型目录。所有目录模型进入采集计划；没有模型抽样或数量截断选项。
- 模型目录的采集时点是本轮锚点。每份响应另存实际请求/接收时间，不能解释为所有数据同时产生。
- JSON、HTML 等原响应按原字节压缩保存。未识别的新字段仍保留在原件中。
- 每次失败与重试都有记录。目录模型数、请求计划数和成功数分开报告；HTTP 成功也需要检查返回结构。
- 业务日期、缓存时间、抓取日期分别处理。缺失不填零，版本缺失不推定 standard；整段返回窗口都保留，以追踪迟到修订。
- 原始响应不提交进 Git 历史。Release 附件作为云端缓冲，本地校验后的原件为长期保存副本；初版不自动删除云端数据。
- 本地下载与入表各有回执。原件已经下载但提取失败时，可在修复程序后重新提取。

## 运行

Python 3.11 或更新版本，采集、打包、下载模块使用标准库。配置文件保留在仓库的 `config` 下；从仓库根目录运行：

```powershell
$env:PYTHONPATH = Join-Path (Get-Location) 'src'
python -m or_pipeline.collector --output runs/my-run
python -m or_pipeline.package build --batch runs/my-run --out dist/my-run
python -m or_pipeline.sync --repository lyll23/or_new --root ../
```

每次真实采集均执行完整模型计划。`tests` 中的合成数据用于检查重复导入、损坏包、缺值与版本等边界，不能作为研究数据。

```powershell
$env:PYTHONPATH = Join-Path (Get-Location) 'src'
python -m unittest discover -s tests -v
```

## 定时与本地接收

云端拟每天 UTC 02:23（北京时间 10:23）运行，也支持手动触发。GitHub 定时可能延迟；实际开始与响应时间写入批次元信息。公开仓库长期无活动时定时任务可能被 GitHub 自动停用，状态应以 Actions 页面为准。

本地 Windows 接收程序在登录后及在线期间定期检查，补拉全部未接收批次。电脑关机不影响 GitHub 采集，但本地接收需要开机并联网。安装入口见 `windows`。安装与首次联网验证完成前，不应把这些文件的存在当作已经启用自动接收。

## 数据解释

当前目标是公开页面/API 可取得的完整响应，不等于平台后台所有数据，也不保证已下架模型仍可访问。接口变更、未授权和数据缺失会在质量报告中列明，不会构造零值补齐。

`count` 是否仅为成功请求、媒体计数单位和工具错误分母等未确认口径继续标为待核实。派生层仅在有原结构依据时投影字段；原始新字段不因映射暂未完成而丢失。已有研究表的封存基线不由本程序直接覆盖。

## 官方参考

- [OpenRouter 模型目录](https://openrouter.ai/docs/api/api-reference/models/get-models)
- [GitHub 定时触发说明](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule)
- [GitHub Release 附件限制](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases)

