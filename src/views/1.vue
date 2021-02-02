<template>
  <div style="margin:0 auto;">
    <el-tabs v-model="activeName" @tab-click="handleClick">
      <el-tab-pane label="关键词统计" name="first" style="margin:0 auto;">
        <el-card class="box-card">
          <div slot="header" class="clearfix">
            <span style="float: left; padding: 3px 0">关键词</span>
            <span style="float: right; padding: 3px 0; margin-left: 140px" >微博数量</span>
            <span style="float: right; padding: 3px 0">情感值</span>
          </div>
          <div class="text item" style="display: inline-flex">
            <span>疫情</span>
            <div style="width: 180px; margin-left: 100px">
              <el-progress :percentage="67.9" :stroke-width ="5"></el-progress>
            </div>
            <span style="float: right; padding: 3px 0; margin-left: 75px" >200051</span>
          </div>
          <div class="text item" style="display: inline-flex">
            <span>北京</span>
            <div style="width: 180px; margin-left: 100px">
              <el-progress :percentage="67.2" :stroke-width ="5"></el-progress>
            </div>
            <span style="float: right; padding: 3px 0; margin-left: 75px" >46190</span>
          </div>

          <div class="text item" style="display: inline-flex">
            <span>新冠</span>
            <div style="width: 180px; margin-left: 100px">
              <el-progress :percentage="67.8" :stroke-width ="5"></el-progress>
            </div>
            <span style="float: right; padding: 3px 0; margin-left: 75px" >35960</span>
          </div>
          <div class="text item" style="display: inline-flex">
            <span>肺炎</span>
            <div style="width: 180px; margin-left: 100px">
              <el-progress :percentage="51.6" :stroke-width ="5"></el-progress>
            </div>
            <span style="float: right; padding: 3px 0; margin-left: 75px" >35660</span>
          </div>
          <div class="text item" style="display: inline-flex">
            <span>防控</span>
            <div style="width: 180px; margin-left: 100px">
              <el-progress :percentage="71.1" :stroke-width ="5"></el-progress>
            </div>
            <span style="float: right; padding: 3px 0; margin-left: 75px" >33256</span>
          </div>
          <div class="text item" style="display: inline-flex">
            <span>病例</span>
            <div style="width: 180px; margin-left: 100px">
              <el-progress :percentage="51.5" :stroke-width ="5"></el-progress>
            </div>
            <span style="float: right; padding: 3px 0; margin-left: 75px" >30817</span>
          </div>
          <div class="text item" style="display: inline-flex">
            <span>中国</span>
            <div style="width: 180px; margin-left: 100px">
              <el-progress :percentage="83" :stroke-width ="5"></el-progress>
            </div>
            <span style="float: right; padding: 3px 0; margin-left: 75px" >30380</span>
          </div>
          <div class="text item" style="display: inline-flex">
            <span>工作</span>
            <div style="width: 180px; margin-left: 100px">
              <el-progress :percentage="70.8" :stroke-width ="5"></el-progress>
            </div>
            <span style="float: right; padding: 3px 0; margin-left: 75px" >27179</span>
          </div>
          <div class="text item" style="display: inline-flex">
            <span>确诊</span>
            <div style="width: 180px; margin-left: 100px">
              <el-progress :percentage="41.4" :stroke-width ="5"></el-progress>
            </div>
            <span style="float: right; padding: 3px 0; margin-left: 75px" >24117</span>
          </div>
          <div class="text item" style="display: inline-flex">
            <span>链接</span>
            <div style="width: 180px; margin-left: 100px">
              <el-progress :percentage="73.4" :stroke-width ="5"></el-progress>
            </div>
            <span style="float: right; padding: 3px 0; margin-left: 75px" >22459</span>
          </div>
          <div class="text item" style="display: inline-flex">
            <span>美国</span>
            <div style="width: 180px; margin-left: 100px">
              <el-progress :percentage="87" :stroke-width ="5"></el-progress>
            </div>
            <span style="float: right; padding: 3px 0; margin-left: 75px" >20842</span>
          </div>
          <div class="text item" style="display: inline-flex">
            <span>爱</span>
            <div style="width: 180px; margin-left: 115px">
              <el-progress :percentage="89.5" :stroke-width ="5"></el-progress>
            </div>
            <span style="float: right; padding: 3px 0; margin-left: 75px" >18915</span>
          </div>
          <div class="text item" style="display: inline-flex">
            <span>希望</span>
            <div style="width: 180px; margin-left: 100px">
              <el-progress :percentage="77.4" :stroke-width ="5"></el-progress>
            </div>
            <span style="float: right; padding: 3px 0; margin-left: 75px" >18172</span>
          </div>
          <div class="text item" style="display: inline-flex">
            <span>新增</span>
            <div style="width: 180px; margin-left: 100px">
              <el-progress :percentage="35.9" :stroke-width ="5"></el-progress>
            </div>
            <span style="float: right; padding: 3px 0; margin-left: 75px" >16080</span>
          </div>
        </el-card>
      </el-tab-pane>
      <el-tab-pane label="热门话题统计" name="second" style="margin:0 auto;">
        <el-table
          :data="tableData"
          style="width: 100%">
          <el-table-column
            prop="date"
            label="话题"
            width="300">
          </el-table-column>
          <el-table-column
            prop="name"
            label="关注人数"
            width="180">
          </el-table-column>
          <el-table-column
            prop="address"
            label="情感值"
            width="150">
          </el-table-column>
        </el-table>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script>
export default {
name: "1",
  data() {
    return {
      activeName: 'second',
      tableData: [{
        date: '张文宏称第二波疫情是必然的',
        name: '520',
        address: '0.392'
      }, {
        date: '跟新浪看热点',
        name: '476',
        address: '0.818'
      }, {
        date: '创造营2019',
        name: '473',
        address: '0.999'
      }, {
        date: '战疫打卡行动',
        name: '445',
        address: '0.636'
      }, {
        date: '武汉加油',
        name: '433',
        address: '0.882'
      }, {
        date: '中诺口腔的红包',
        name: '432',
        address: '0.983'
      }, {
        date: '天津疫情',
        name: '408',
        address: '0.143'
      }, {
        date: '张文宏称疫情今夏结束基本已不可能',
        name: '334',
        address: '0.25'
      }, {
        date: '吴尊友说北京疫情已经控制住了',
        name: '307',
        address: '0.412'
      }, {
        date: '新闻看点',
        name: '284',
        address: '0.766'
      }, {
        date: '成都疫情',
        name: '258',
        address: '0.169'
      }, {
        date: '疫情过后你最想见的人',
        name: '254',
        address: '0.294'
      }, {
        date: '最美逆行人',
        name: '248',
        address: '0.907'
      }, {
        date: '最美逆行者之幸福社区',
        name: '247',
        address: '0.911'
      }]
    };
  },
  methods: {
    handleClick(tab, event) {
      console.log(tab, event);
    }
  }
}
</script>

<style>
.text {
  font-size: 14px;
}

.item {
  margin-bottom: 18px;
}

.clearfix:before,
.clearfix:after {
  display: table;
  content: "";
}
.clearfix:after {
  clear: both
}

.box-card {
  width: 480px;
}
</style>
