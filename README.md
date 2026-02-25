### lombok注解
   
   工程中大量使用了lombok注解，未安装lombok插件会导致无法编译，所以请自行百度安装lombok插件
   
   主要使用了如下注解：
   
   + `@Data` 此注解会自动生成`Getter`,`Setter`,`toString`等方法
   + `@Accessors(chain = true)` 此注解会让`Setter`方法返回`this`，以便链式调用
   + `@Accessors(fluent = true)` 此注解会让生成的`Getter`,`Setter`方法不带`Get`和`Set`，仅在`Tuple`数据结构中使用
   + `@AllArgsConstructor` 此注解会生成一个全参的构造器，仅在只有两个成员的POJO类中使用
   + `@NoArgsConstructor` 此注解会生成一个无参数的构造器
   
### 工程构建
由于工程中存在JAR包冲突，故简单的通过IDE打包后`METE-INF`文件夹中会产生`*.SF`,`*.DSA`,`*.RSA`文件，
[导致运行Jar包时产生数字签名错误。](http://stackoverflow.com/questions/999489/invalid-signature-file-when-attempting-to-run-a-jar)
所以本工程用`maven`构建，`pom.xml`中配置了`maven`的`shade`插件，可以在打包时
过滤掉数字签名文件，也能配置自动精简Jar包（慎用）。

具体操作：
+ 最新版本的IDEA下（2018.3），直接双击Ctrl，然后`mvn package`或者`mvn clean package`即可。如果是第一次执行则需要下载插件。如果
发现插件下载不动则请配置一下 `mirror`，具体怎么配置自行百度。
+ 若工程中提供了测试用例，那么构建时会自动运行测试用例，加`-DskipTests`参数来跳过测试用例
+ 若不是最新版的IDEA，则需要配置maven的环境变量，然后在Terminal中输入命令。或者下载IDEA的 `Maven Helper`插件，下载安装完毕后
右键就会有执行`maven`命令的选项。
+ `eclipse`自行研究
+ 如果就是不想用构建工具，非要用IDE打包，那么打包完后手动删掉那几个签名文件也行

### 建议了解一下的库
+ [`RxJava`](http://reactivex.io/)和[`Project Reactor`](https://projectreactor.io/) ：这两个库是为响应式函数式编程准备的，
将异步抽象为事件流，统一了回调/多线程/单线程/协程的写法。不过学习曲线稍微有点陡峭。
+ [`Eclipse Collection`](http://www.eclipse.org/collections/) ：这是一个集合框架，
相较于JDK自带的集合框架，`Eclipse Collection`拥有更快的速度，更高的内存利用率以及更丰富的函数式API。
+ [`Lettuce`](https://lettuce.io/) ：这是基于`Netty`的`Redis`客户端，可以说是全面碾压`Jedis`。`Lettuce`是线程安全的，
大部分情况下可以线程间复用，自带了重试/故障切换/拓扑发现等功能，还支持`Unix Domain Socket`等高级功能。
操作方面，`Lettuce`提供了同步/异步/响应式三套API，其中响应式对接了`Project Reactor`。难度方面
**响应式 >> 异步 > 同步**。

### IDEA插件
这里推荐一些 `IntelliJ IDEA`的插件，`eclipse`应该也有部分可以用，不做保证。
+ `Key PromoterX` 可以帮你熟悉所有的快捷键，也能提醒你将常用操作设置成快捷键
+ `IDEA Feature Trainer`可以帮你熟悉IDEA的各种功能
+ `Statistic`可以帮你对工程进行统计，例如代码行数，注释行数，空行等等
+ `Alibaba Java Codeing GuideLines`阿里的**编程规范**插件，帮你检查不规范的地方，强烈建议安装
+ `SonarLint`代码质量管理工具，可以扫描出很多问题，强烈建议安装
+ `Background Image Plus`等美化插件，感兴趣可以装一个玩一下

### 一些简单的教学
#### 前言
在历史上，Java有若干个非常重要的版本。第一个就是JDK1.2，发布于1998年。
JDK1.2首次引入了JIT（`及时编译`），在此之前Java都是解释执行的（和Python一样），性能奇差。接下来便是JDK1.4和JDK1.5。
JDK1.4背后有许多大公司的支持，性能得到了大幅优化，JDK1.5则是加入了很多新的语法特性，例如注解，变长参数，泛型，自动装箱等等。
后来由于各种乱七八糟的原因，原本打算在JDK1.7发布的大量新特性大都泡汤了，最后Oracle出现，收购了Sun公司。于是乎，JDK1.8便自然而然的
继承了原本打算在JDK1.7发布的新特性，成为了Java历史上的一道分水岭。至于JDK1.8具体有哪些新特性，后文再细说。

虽然目前主流的Java版本是JDK1.8，但是要知道，JDK1.8已经是2014年的东西了。那么是不是大家都不思进取不愿意接受新事物呢？当然不是，这和Oracle的策略有关。
Oracle收购Sun之后，Java分为了LTS版本（`长期支持版本`）和功能性版本，其中功能性版本Oracle仅提供半年的支持，相当于一个过渡，LTS版本则提供至少三年的支持。
而目前主流的JDK8则是一个LTS版本，后续的9,10都是功能性版本。当然了，有人可能会说，我没体会到Oracle提供的支持啊？我电脑上的JDK从安装到现在就没更新过。其实
Oracle和OpenJDK社区在背后的付出是咱们无法想象的，他们对JVM的优化是你调一辈子参数都比不上的，而获得这些优化的方法很简单，简单到你以为这是很廉价的工作，即
更新一下JDK就好了。

随着下一个LTS版本——JDK11的发布，JDK8退出历史舞台也是早晚的事情了（`2019年1月停止提供支持`）。JDK11包含了9和10的新特性，包括模块化，var类型，flow响应式
流，以及StreamAPI的加强。并且裁剪掉了Java EE，Servlet技术栈可以说差不多要走到头了。当然，最重要的还是ZGC，提供了超低延迟的垃圾回收能力。不过其实看起来版本
跨度这么大，其实开发者要学习的内容量远远比不上JDK7到JDK8，要不然为什么说JDK8是一道分水岭呢？

#### JDK8的新特性
1. 泛型推导

    泛型推导估计是我们在IDE帮助下用到的唯一一个新特性了。其实泛型推导在JDK7中就有了，比如如下代码：

        List<String> list = new ArrayList<String>()` 可以写成  `List<String> list = new ArrayList<>()
    右边尖括号中的类型会由编译器帮你推导出来，不需要你再手动指定了。这一功能在JDK8中得到了加强，可以对泛型方法进行泛型推导，官方的例子如下：

        static <T> T pick(T a1, T a2) { return a2; }
        Serializable s = pick("d", new ArrayList<String>());
    而在JDK7中，你需要这样写：

        Serializable s = this.<Serializable>pick("d", new ArrayList<String>());

    看到这里，或许有人会问，这有啥，不就少敲几个字吗。其实不然，这个特性为后面的Lambda表达式，流式api以及函数式编程提供了基础

2. 函数式接口

    函数式接口的定义很简单，只含有一个没有方法体的方法（是的，JDK8中接口的方法可以有方法体了，加上`default`关键字即可）的接口，就是函数式接口。
    其实函数式接口也不是什么新东西了，JDK7中也有了，那就是`Runable` 和 `Callable`。是不是很眼熟？没错，你每次`new`一个`Thread`的时候要实现的
    `run`方法就是`Runable`中的。

3. Lambda表达式

    Lambda表达式其实也很简单，`(a,b)-> a+b` 就代表了一个参数是a和b，返回值是a+b的函数。那么Lambda表达式具体有什么用呢？先来看一个简单的例子：

       Thread thread = new Thread(new Runnable() {
           @Override
           public void run() {
               System.out.println("Hello");
           }
       });
    这种匿名内部类的写法，写了六行其实只有中间一行是有效的，看起来非常丑陋。那么用Lambda表达式是什么样的呢？

        Thread thread = new Thread(() -> System.out.println("Hello"));
    一行就搞定了，非常优雅。如果你用IDEA，并且写出了第一种写法，IDEA会不停的提醒你这么写有问题，并且可以`alt +Enter`一键转换成Lambda表达式。

4. 流式API

    流式API其实就是各种操作符，例如`map`,`flatmap`,`filter`等等。这里不具体介绍操作符（是的，我懒），直接给出实际应用的例子。
    参考请求鉴权的过程，我们有保存所有http请求的键值对的List，即:

        List<Entry<String,String>>

    现在我们要从这个结构中找到`x-amz-`开头的header，将key和value拼起来，然后升序排序，最后加上换行符连接成字符串。常规写法如下：

        FastList<String> resList = new FastList<>();
        for (int i = 0; i < list.size(); i++) {
            Map.Entry<String,String> entry = list.get(i);
            if(entry.getKey().startsWith("x-amz-")) {
                resList.add(entry.getKey() + ":" +entry.getValue());
            }
        }

        resList.sortThis();

        StringBuilder builder = new StringBuilder();
        for (int i1 = 0; i1 < resList.size(); i1++) {
            builder.append(resList.get(i1)).append("\n");
        }
        String res = builder.toString();

    那么用Stream API之后代码是什么样的呢？

        String res = list.stream()
                        .filter(entry -> entry.getKey().startsWith("x-amz-"))
                        .map(entry -> entry.getKey() + ":" + entry.getValue())
                        .sorted()
                        .collect(Collectors.joining("\n"));

    这还只是一个非常简单的例子，思考一下，如果`List`中又是`List`呢？嵌套是不是又多了一层。这几层嵌套搞下去，如果不加注释，代码放几个月之后自己看起来都吃力。
    而流式计算则可以把嵌套展平，把代码变成非常清晰的链式调用。

    当然，Stream也并非银弹，虽然在接口测试中两种写法对QPS没有影响，但是在微基准测试中还是有区别的。在List长度为10的情况下，Stream要比for循环慢0.25us。
    如果不是强迫症的话，这么点差距基本可以忽略了。但是如果非要追求极限性能，可以考虑更新JDK。


