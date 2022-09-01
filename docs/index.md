---
layout: article
title: Apache Arrow 中文文档
sidebar:
   nav: navigator
---

## Apache Arrow 中文文档

### 前言

本文档是我为了记录Apache Arrow学习过程写下的文档，基于9.0.0的官方文档翻译和验证而成。因此可能出现不全、不对、不及时的情况，个人精力能力有限，请谅解。

### 代码仓库

[https://github.com/ZhengqiaoWang/ArrowDocsZhCN](https://github.com/ZhengqiaoWang/ArrowDocsZhCN)

### 个人想法

ApacheArrow我个人认为是一个比较适合分析处理大量数据的框架，它不像numpy一样需要将数据先加载到内存中再进行处理计算，而是在数据读取时就根据需要将数据异步读取，这样的确极大的减少了内存占用。不可否认，在一些简单的计算场景下，ApacheArrow因为采取了类似yield的读取机制（Generator），其节省的内存拷贝时间能极大的缩短数据加载计算的整个流程，但这并不意味着其纯粹的内存计算与采用类似内存架构的pandas计算速度更快，这一点还是值得商榷的。

不可否认的是，如果你需要分析手中的大量数据，同时苦恼于内存远不及数据量时，ApacheArrow是目前我认为最优的选择，他可以节省更多精力更多时间，让你更快速的处理大批量数据，并分析出想要的结果。

### 如何贡献

可以提交Issue或Fork+PR。
