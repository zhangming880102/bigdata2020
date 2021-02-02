<template>
  <div>
    <el-row>
      <el-col :span="24">
        <div class="grid-content">
          <el-input
              size="medium"
              style="width: 40%"
              placeholder="请输入内容"
              v-model="input"
              clearable>
            <template slot="prepend">请输入用户编号:</template>
          </el-input>
          <el-button type="primary" size="medium" @click="onSubmit">查询</el-button>
        </div>
      </el-col>
    </el-row>
    <el-row>
      <el-col :span="12">
        <el-table
          :data="tableData1"
          border
          height="340"
          :row-style="{height:0+'px'}"
          style="width: 100%">
          <el-table-column label="推荐用户" align="center">
          <el-table-column
            fixed
            prop="userid"
            label="用户ID"
            align="center"
            width=":33%">
          </el-table-column>
            <el-table-column
                fixed
                prop="petname"
                label="用户昵称"
                align="center"
                width=":33%">
            </el-table-column>
          <el-table-column
              prop="degree"
              label="推荐度"
              align="center"
              width="auto">
          </el-table-column>
          </el-table-column>
        </el-table>
      </el-col>
      <el-col :span="12">
        <el-table
            :data="tableData2"
            border
            height="340"
            :row-style="{height:0+'px'}"
            style="width: 100%">
          <el-table-column label="推荐话题" align="center">
            <el-table-column
                fixed
                prop="topic"
                label="话题"
                align="center"
                width=":33%">
            </el-table-column>
            <el-table-column
                prop="degree"
                label="推荐度"
                align="center"
                width="auto">
            </el-table-column>
          </el-table-column>
        </el-table>
      </el-col>
    </el-row>


  </div>
</template>
<style>
.el-row {
  margin-bottom: 5px;
&:last-child {
   margin-bottom: 0;
 }
}
.el-col {
  border-radius: 4px;
}
.bg-purple-dark {
  background: #99a9bf;
}
.bg-purple {
  background: #d3dce6;
}
.bg-purple-light {
  background: #e5e9f2;
}
.grid-content {
  border-radius: 4px;
  min-height: 36px;
}
.row-bg {
  padding: 10px 0;
  background-color: #f9fafc;
}
</style>

<script>
export default {
  data() {
    return {
      input: '',
      tableData1: [{
        userid: 2250632037,
        petname:"_Donnawu",
        degree:0.389
      },{
        userid: 6411588036,
        petname:"我是炸鸡酱",
        degree:0.348
      },{
        userid: 6408470484,
        petname:"路得0001",
        degree:0.338
      },{
        userid: 1932234663,
        petname:"糖心的洋葱",
        degree:0.335
      },{
        userid: 5628233904,
        petname:"明天那只鹤",
        degree:0.335
      },{
        userid: 3494639642,
        petname:"深深情姑娘",
        degree:0.324
      },{
        userid: 5960399079,
        petname:"快乐宝贝七",
        degree:0.320
      }
      ],
      tableData2: [
        {
          topic:"成都一女子强行翻越隔离墙",
          degree:2.623
        },{
          topic:"特朗普回应美国死亡病例突破20万",
          degree:1.749
        },{
          topic:"北京某餐馆7人确诊新冠肺炎",
          degree:1.749
        },{
          topic:"便民提示",
          degree:1.389
        },{
          topic:"我在北京等你",
          degree:1.302
        },{
          topic:"北京旅事",
          degree:1.226
        },{
          topic:"北京新增5例新型肺炎病例",
          degree:1.205
        },
      ]
    }
  }
}
</script>
