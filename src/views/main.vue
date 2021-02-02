<template>
  <el-container class="main_container">
    <el-header>
      <div class="left_box">
        <img src="../assets/img/p1.jpg">
        <span>
          云计算分析系统
        </span>
      </div>
      <div class="right_box">
        <el-dropdown>
          <img src="../assets/img/p1.jpg" class="el-icon-arrow-down">
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item icon="el-icon-house">系统首页</el-dropdown-item>
            <el-dropdown-item icon="el-icon-ship">交流讨论</el-dropdown-item>
            <el-dropdown-item icon="el-icon-switch-button">退出登录</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
      </div>
    </el-header>
    <el-container>
<!--      侧边栏-->
      <el-aside :width="isCollapsed?'64px':'200px'">
<!--        展开或者收起-->
        <div class="toggle_box" @click="toggleCollapse">|||</div>
        <el-menu
          default-active="activePath"
          class="el-menu-vertical-demo"
          @open="handleOpen"
          @close="handleClose"
          background-color="#001529"
          text-color="#fff"
          active-text-color="#ffd04b"
          :collapse="isCollapsed"
          :collapse-transition="false"
          :unique-opened="true"
          :router="true">
          <MenuTree :menuList="this.menuList"></MenuTree>
        </el-menu>
      </el-aside>
      <el-main>
<!--        路由视图-->
        <router-view></router-view>
      </el-main>
    </el-container>
  </el-container>
</template>

<script>
import MenuTree from '../components/MenuTree'
export default {
  name: 'main',
  data () {
    return {
      isCollapsed: false,
      activePath: '',
      menuList:[
        {
          "id": 1,
          "parentId": 0,
          "menuName": "数据分析",
          "url": "",
          "icon": "el-icon-setting",
          "orderNum": 1,
          "open": 1,
          "disabled": false,
          "perms": null,
          "type": 0,
          "children": [
            {
              "id": 261,
              "parentId": 1,
              "menuName": "分词情感分析",
              "url": "/1",
              "icon": "el-icon-s-home",
              "orderNum": 2,
              "open": 0,
              "disabled": false,
              "perms": "",
              "type": 0,
              "children": []
            },
            {
              "id": 261,
              "parentId": 1,
              "menuName": "用户推荐查询",
              "url": "/2",
              "icon": "el-icon-s-home",
              "orderNum": 2,
              "open": 0,
              "disabled": false,
              "perms": "",
              "type": 0,
              "children": []
            },
            {
              "id": 261,
              "parentId": 1,
              "menuName": "相似图片搜索",
              "url": "/3",
              "icon": "el-icon-s-home",
              "orderNum": 3,
              "open": 0,
              "disabled": false,
              "perms": "",
              "type": 0,
              "children": []
            },
            {
              "id": 320,
              "parentId": 1,
              "menuName": "公告管理",
              "url": "/notices",
              "icon": "el-icon-s-flag",
              "orderNum": 3,
              "open": 0,
              "disabled": true,
              "perms": "",
              "type": 0,
              "children": []
            }
          ]
        },
        {
          "id": 312,
          "parentId": 0,
          "menuName": "数据预测",
          "url": null,
          "icon": "el-icon-s-goods",
          "orderNum": 2,
          "open": 0,
          "disabled": false,
          "perms": null,
          "type": 0,
          "children": [
            {
              "id": 229,
              "parentId": 312,
              "menuName": "房价预测",
              "url": "/predict",
              "icon": "el-icon-date",
              "orderNum": 1,
              "open": 1,
              "disabled": false,
              "perms": "el-icon-date",
              "type": 0,
              "children": []
            }
          ]
        }
      ]
    }
  },
  components: {
    MenuTree
  },
  methods: {
    toggleCollapse () {
      this.isCollapsed = !this.isCollapsed
    }
  }
}
</script>

<style lang = 'less' scoped>
.main_container{
  height: 100%;
}
.el-header {
  background-color: #001529;
  display: flex;
  justify-content: space-between;
  color: #ffffff;
  padding-left: 0;
  align-items: center;
  font-size: 20px;
  .left_box{
    display: flex;
    align-items: center;
    img{
      height: 50px;
      width: 50px;
      margin: 0px 0px 0px 10px;
    }
    span{
      margin-left: 20px;
    }
  }
  .right_box{
    .el-dropdown>img{
      width: 60px;
      height: 60px;
      border-radius: 50%;
      background-color: #ffffff;
      margin: 5px 15px 0px 0px;
      background-size: contain;
    }
    display: flex;
    align-items: center;
  }
}

.el-aside {
  background-color: #001529;
  .el-menu{
    border-right: none;
  }
  .toggle_box{
    background-color: chartreuse;
    font-size: 15px;
    font-weight: bold;
    line-height: 24px;
    color: #ffffff;
    text-align: center;
    letter-spacing: 0.2em;
    cursor: pointer;
  }
}

.el-main {
  background-color: #E9EEF3;
}

body > .el-container {
  margin-bottom: 40px;
}

.el-dropdown-link {
  cursor: pointer;
  color: #409EFF;
}
.el-icon-arrow-down {
  font-size: 12px;
}

</style>
