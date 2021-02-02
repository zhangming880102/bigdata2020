<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <link rel="stylesheet" href="https://unpkg.com/element-ui@2.14.1/lib/theme-chalk/index.css">
<script src="https://unpkg.com/vue/dist/vue.js"></script>
<script src="https://unpkg.com/element-ui@2.14.1/lib/index.js"></script>
<!-- <script src="https://cdn.bootcss.com/vuex/3.0.1/vuex.min.js"></script>
  <script src="https://cdn.bootcss.com/vue-router/3.0.1/vue-router.min.js"></script>
  <script src="https://cdn.bootcss.com/axios/0.19.2/axios.min.js"></script> -->
<style>
    .hide .el-upload--picture-card {
        display: none;
    }
</style>
</head>

<body>


<div id="app">
<el-upload
                action="https://jsonplaceholder.typicode.com/posts/"
                list-type="picture-card"
                :on-preview="handlePictureCardPreview"
                :on-remove="handleRemove" 图片移除事件
                :limit="1"
                :on-change="handleChange" 图片上传事件
                :class="{ hide: hideUpload }" 设置一个class表达式用于达到条件调用隐藏样式>
                <i class="el-icon-plus"></i>
                <div slot="tip" class="el-upload__tip">只能上传jpg/png文件，且不超过40kb</div>
              </el-upload>
              <el-dialog :visible.sync="dialogVisible">
                <img width="100%" :src="dialogImageUrl" alt="" />
              </el-dialog>
</div>

<p>

</p>
<div id="app2">

<el-button type="primary" icon="el-icon-share"></el-button>
<el-button type="primary" icon="el-icon-search"  @click=show()>搜索</el-button>
</div>
<div id="app3" style="display:none;">
<div>
    找到相关图片共1563张:
  <el-link type="primary">版权</el-link>
    <el-link type="primary">高清</el-link>
    <el-link type="primary">最新</el-link>
    <el-link type="primary">动图</el-link>
    <el-link type="primary">全部尺寸</el-link>
</div>
</div>
<div id="app4" style="display:none;">
<div class="demo-image">
  <div class="block" v-for="fit in fits" :key="fit">

    <el-image v-for="url in urls" :key="url"
      style="width: 300px; height: 200px"
      :src="url"
      :fit="fit"></el-image>
  </div>
</div>
</div>


</body>
<script>

    var Main2 = {
    data() {
      return {
        dialogImageUrl: '',
          dialogVisible:false,
          hideUpload:false　,
　　　　　limitCount:1
　　　　　　　　
      };
    },
    methods: {
        handleRemove(file, fileList) {
      this.hideUpload = fileList.length >= this.limitCount;
    },
    handleChange(fileList){
      this.hideUpload = fileList.length >= this.limitCount;
      this.hideUpload = true; //此时值为ture时 才会执行隐藏
    },
      handlePictureCardPreview(file) {
        this.dialogImageUrl = file.url;
        this.dialogVisible = true;
      }
    }
  }
var Ctor = Vue.extend(Main2)
new Ctor().$mount('#app')
     var buttonmethod ={
        data() {
      return {
　　　　　　　　
      };
    },
         methods:{
            show(){
        var div3=document.getElementById("app3");
         div3.style.display="";
         var div4=document.getElementById("app4");
         div4.style.display="";
     }
         }
     }
    var Ctor = Vue.extend(buttonmethod)
   new Ctor().$mount('#app2')

    new Vue().$mount('#app3')
    var imageshow = {
        data() {
      return {
         fits: ['fill', 'contain', 'cover', 'none', 'scale-down'],
        urls: [
          'https://fuss10.elemecdn.com/a/3f/3302e58f9a181d2509f3dc0fa68b0jpeg.jpeg',
          'https://fuss10.elemecdn.com/1/34/19aa98b1fcb2781c4fba33d850549jpeg.jpeg',
          'https://fuss10.elemecdn.com/0/6f/e35ff375812e6b0020b6b4e8f9583jpeg.jpeg',
          'https://fuss10.elemecdn.com/9/bb/e27858e973f5d7d3904835f46abbdjpeg.jpeg',
          'https://fuss10.elemecdn.com/d/e6/c4d93a3805b3ce3f323f7974e6f78jpeg.jpeg',
          'https://fuss10.elemecdn.com/3/28/bbf893f792f03a54408b3b7a7ebf0jpeg.jpeg',
          'https://fuss10.elemecdn.com/2/11/6535bcfb26e4c79b48ddde44f4b6fjpeg.jpeg',
            'data1.jpg',
            'data2.jpg',
            'data3.jpg'
        ]

      }
    }

  }
var Ctor = Vue.extend(imageshow)
new Ctor().$mount('#app4')

    show
</script>

</html>